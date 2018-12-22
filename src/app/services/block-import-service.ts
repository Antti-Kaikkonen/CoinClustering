import { AbstractBatch } from "abstract-leveldown";
import { LevelUp } from "levelup";
import { BlockService } from "./block-service";
import { ClusterAddressService } from "./cluster-address-service";
import { ClusterBalanceService } from "./cluster-balance-service";
import { db_address_cluster_prefix, db_cluster_merged_to, db_last_merged_block_height, db_last_saved_tx_height, db_last_saved_tx_n, db_next_cluster_id } from "./db-constants";

export class BlockImportService {

  constructor(private db: LevelUp,
    private clusterAddressService: ClusterAddressService, 
    private clusterBalanceService: ClusterBalanceService,
    private blockService: BlockService) {

  }  

  lastMergedHeight: number;
  lastSavedTxHeight: number;
  lastSavedTxN: number;
  nextClusterId: number;

  private getTransactionAddressBalanceChanges(tx): Map<string, number> {
    let addressToDelta = new Map<string, number>();
    tx.vin.filter(vin => vin.address)
    .forEach(vin => {
      let oldBalance = addressToDelta.get(vin.address);
      if (!oldBalance) oldBalance = 0;
      addressToDelta.set(vin.address, oldBalance-vin.valueSat);
    }); 
    tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1)
    .forEach(vout => {
      let oldBalance = addressToDelta.get(vout.scriptPubKey.addresses[0]);
      if (!oldBalance) oldBalance = 0;
      addressToDelta.set(vout.scriptPubKey.addresses[0], oldBalance+vout.valueSat);
    });
    return addressToDelta;
  }
  
  private async addressBalanceChangesToClusterBalanceChanges2(addressToDelta: Map<string, number>, addressToClusterId: Map<string, number>): Promise<Map<string, number>> {
    let promises = [];
    let addresses = [];
    addressToDelta.forEach((delta: number, address: string) => {
      addresses.push(address);
      if (addressToClusterId.has(address)) {
        promises.push(new Promise((resolve, reject) => resolve(addressToClusterId.get(address))));
      } else {
        promises.push(this.db.get(db_address_cluster_prefix+address));
      }
      //promises.push(this.db.get(db_address_cluster_prefix+address));
    });
    let clusterIds = await Promise.all(promises);
    let clusterToDelta = new Map<string, number>();
    addresses.forEach((address: string, index: number) => {
      let clusterId = clusterIds[index];
      let oldBalance = clusterToDelta.get(clusterId);
      let addressDelta = addressToDelta.get(address);
      if (!oldBalance) oldBalance = 0;
      clusterToDelta.set(clusterId, oldBalance+addressDelta);
    });
    return clusterToDelta;
  }

  private async addressBalanceChangesToClusterBalanceChanges(addressToDelta: Map<string, number>): Promise<Map<string, number>> {
    let promises = [];
    let addresses = [];
    addressToDelta.forEach((delta: number, address: string) => {
      addresses.push(address);
      promises.push(this.db.get(db_address_cluster_prefix+address));
    });
    let clusterIds = await Promise.all(promises);
    let clusterToDelta = new Map<string, number>();
    addresses.forEach((address: string, index: number) => {
      let clusterId = clusterIds[index];
      if (clusterId === undefined) throw Error("Cluster missing");
      let oldBalance = clusterToDelta.get(clusterId);
      let addressDelta = addressToDelta.get(address);
      if (!oldBalance) oldBalance = 0;
      clusterToDelta.set(clusterId, oldBalance+addressDelta);
    });
    return clusterToDelta;
  }
  
  private txAddressesToCluster(tx): Set<string> {
    let result = new Set<string>();
    if (this.isMixingTx(tx)) return result;
    tx.vin.map(vin => vin.address).filter(address => address !== undefined).forEach(address => result.add(address));
    return result;
  }
  
  private txAddresses(tx): Set<string> {
    let result = new Set<string>();
    tx.vin.map(vin => vin.address).filter(address => address !== undefined).forEach(address => result.add(address));
    tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1)
    .map(vout => vout.scriptPubKey.addresses[0]).forEach(address => result.add(address));
    return result;
  }

  private isMixingTx(tx) {
    if (tx.vin.length < 2) return false;
    if (tx.vout.length !== tx.vin.length) return false;
    let firstInput = tx.vin[0];
    if (typeof firstInput.valueSat !== 'number') return false;
    if (!tx.vin.every(vin => vin.valueSat === firstInput.valueSat)) return false;
    if (!tx.vout.every(vout => vout.valueSat === firstInput.valueSat)) return false;
    return true;
  }
  
  private async getAddressClusterInfo(address: string) {
    return new Promise<{address: string, clusterId?: number}>((resolve, reject) => {
      this.db.get(db_address_cluster_prefix+address, (error, clusterId: string) => {
        if (clusterId !== undefined) {
          resolve( { address: address, clusterId: Number(clusterId) });
        } else {
          resolve({address: address});
        }
      });
    });
  }

  async processClusters(clusters: Cluster[], lastBlockHeight: number) {
    let ops: AbstractBatch[] = [];
    let promises = [];
    let i = 0;
    for (let cluster of clusters) {
      i++;
      let clusterIds: number[] = Array.from(cluster.clusterIds).sort((a, b) => a-b);
      let clusterAddresses: string[] = Array.from(cluster.addresses);
      if (clusterIds.length === 0) {
        promises.push(this.clusterAddressService.createAddressClustersOps(clusterAddresses, await this.getNextClusterId()));
        this.nextClusterId++;
      } else {
        let toClusterId = clusterIds[0];
        let fromClusters = clusterIds.slice(1);
        if (fromClusters.length > 0 || clusterAddresses.length > 0) {
          console.log(lastBlockHeight.toString(),"merging to",toClusterId,"from",fromClusters.join(","));
          promises.push(this.clusterAddressService.mergeClusterAddressesOps(toClusterId, fromClusters, clusterAddresses));
          if (fromClusters.length > 0) promises.push(this.clusterBalanceService.mergeClusterTransactionsOps(toClusterId, ...fromClusters));
          fromClusters.forEach(fromClusterId => {
            ops.push({
              type:"put",
              key:db_cluster_merged_to+fromClusterId,
              value:toClusterId
            })
          });
        }
      }  
    }  
    let v = await Promise.all(promises);
    v.forEach(opGroup => opGroup.forEach(op => ops.push(op)));
    let a = (new Date).getTime();

    ops.push({
      type:"put",
      key:db_last_merged_block_height,
      value:lastBlockHeight
    });
    ops.push({
      type:"put",
      key:db_next_cluster_id,
      value: await this.getNextClusterId()
    });

    if (ops.length > 1000) console.log("ops.length: ", ops.length);
    await this.db.batch(ops);
  }


  private async computeClusters(block): Promise<Cluster[]> {
    let txs = block.tx;
    

    let a = (new Date).getTime();


    let allAddresses: Set<string> = new Set();
    let txToAddress: Map<number, Set<string>> = new Map();
    let txToAddressesNotToCluster: Map<number, Set<string>> = new Map();
    let txToAddressesToCluster: Map<number, Set<string>> = new Map();
    for (const [index, tx] of txs.entries()) {
      let txAddresses = this.txAddresses(tx);
      if (txAddresses.size > 0) txToAddress.set(index, new Set());
      txAddresses.forEach(address => {
        allAddresses.add(address);
        txToAddress.get(index).add(address);
      });

      let addressesToCluster = this.txAddressesToCluster(tx);
      if (addressesToCluster.size > 0) txToAddressesToCluster.set(index, new Set());
      addressesToCluster.forEach(address => {
        txToAddressesToCluster.get(index).add(address);
      });
      
      txAddresses.forEach(address => {
        if (!addressesToCluster.has(address)) {
          if (!txToAddressesNotToCluster.has(index)) txToAddressesNotToCluster.set(index, new Set());
          txToAddressesNotToCluster.get(index).add(address);
        }
      });
    } 
    let addressToClusterPromises: Promise<{address: string, clusterId?: number}>[] = [];
    for (let address of allAddresses) {
      addressToClusterPromises.push(this.getAddressClusterInfo(address));
    } 
    let addressesWithClusterInfo = await Promise.all(addressToClusterPromises);
    let addressToClusterId: Map<string, number> = new Map();
    addressesWithClusterInfo.forEach(v => {
      if (v.clusterId === undefined) {
      } else {
        addressToClusterId.set(v.address, v.clusterId);
      }
    });
    let newClusters: Cluster[] = [];
    txToAddressesToCluster.forEach((addresses: Set<string>, txN: number) => {
      let txCluster: Cluster = new Cluster(); 
      newClusters.push(txCluster);
      addresses.forEach(address => {
        if (addressToClusterId.has(address)) {
          let clusterId = addressToClusterId.get(address);
          txCluster.clusterIds.add(clusterId);
        } else {
          txCluster.addresses.add(address);
        }
      });
    });

    for (let i = 0; i < newClusters.length; i++) {
      let ii = 0;
      let clusterA = newClusters[i];
      while (ii < newClusters.length) {
        let clusterB = newClusters[ii];
        if (ii !== i && clusterA.intersectsWith(clusterB)) {
          clusterA.mergeFrom(clusterB);
          newClusters.splice(ii, 1);
        } else {
          ii++;
        }
      }
    }
    for (let i = 0; i < txs.length; i++) {
      let txAddresses = txToAddress.get(i);
    }

    for (let i = 0; i < txs.length; i++) {
      let txAddresses = txToAddressesNotToCluster.get(i);
      if (txAddresses === undefined) continue;
      txAddresses.forEach(address => {
        if (addressToClusterId.has(address)) return;//address already in a cluster. If the cluster should be combined then it is already in newClusters
        let clusterContainingAddress = newClusters.find(cluster => cluster.addresses.has(address));
        if (clusterContainingAddress !== undefined) {
        } else {
          newClusters.push(new Cluster(new Set([address])));
        }
      });  
    }
    return newClusters;
  }

  async blockMerging(block) {
    if (block.height <= this.getLastMergedHeight()) return;
    await this.processClusters(await this.computeClusters(block), block.height);
    this.lastMergedHeight = block.height;
  }

  async saveBlockTransactions(block) {
    if (block.height <= this.getLastSavedTxHeight()) return;//already saved
    let txs = block.tx;

    let clusterBalanceChangesPromises = [];
    for (let tx of txs) {
      let addressBalanceChanges = this.getTransactionAddressBalanceChanges(tx);
      clusterBalanceChangesPromises.push(this.addressBalanceChangesToClusterBalanceChanges(addressBalanceChanges));
    }
    let clusterBalanceChanges = await Promise.all(clusterBalanceChangesPromises);
    for (const [index, tx] of txs.entries()) {
      let lastSavedTxHeight = await this.getLastSavedTxHeight();
      if (block.height > lastSavedTxHeight+1 || index > await this.getLastSavedTxN()) {
        await this.clusterBalanceService.saveClusterBalanceChanges(tx.txid, block.height, index, clusterBalanceChanges[index]);
        this.lastSavedTxN = index;
      }
    }
    this.lastSavedTxHeight = block.height;
    let ops: AbstractBatch[] = [];
    ops.push({
      type: "put",
      key: db_last_saved_tx_height,
      value: block.height
    });
    ops.push({
      type: "del",
      key: db_last_saved_tx_n
    });
    await this.db.batch(ops);
    this.lastSavedTxN = -1;
  }

  async getLastMergedHeight(): Promise<number> {
    if (this.lastMergedHeight === undefined) {
      try {
        this.lastMergedHeight = Number(await this.db.get(db_last_merged_block_height));
      } catch (err) {
        this.lastMergedHeight = -1;
      }
    }
    return this.lastMergedHeight;
  }

  async getLastSavedTxHeight(): Promise<number> {
    if (this.lastSavedTxHeight === undefined) {
      try {
        this.lastSavedTxHeight = Number(await this.db.get(db_last_saved_tx_height));
      } catch (err) {
        this.lastSavedTxHeight = -1;
      }
    }
    return this.lastSavedTxHeight;
  }

  private async getLastSavedTxN(): Promise<number> {
    if (this.lastSavedTxN === undefined) {
      try {
        this.lastSavedTxN = Number(await this.db.get(db_last_saved_tx_n));
      } catch (err) {
        this.lastSavedTxN = -1;
      }
    }
    return this.lastSavedTxN;
  }

  private async getNextClusterId(): Promise<number> {
    if (this.nextClusterId === undefined) {
      try {
        this.nextClusterId = Number(await this.db.get(db_next_cluster_id));
      } catch(err) {
        this.nextClusterId = 0;
      }
    }
    return this.nextClusterId;
  }

  async saveBlock(block) {
    if (block.height%1000 === 0) {
      console.log(block.height);
    }  

    if (block.height > await this.getLastMergedHeight()) {
      await this.blockMerging(block);
      this.lastMergedHeight = block.height;
    }
    //if (true) return;
    if (block.height >= await this.getLastSavedTxHeight()) {
      await this.saveBlockTransactions(block);
    }
    
    //if (true) return;


    /*for (const [txindex, tx] of txs.entries()) {
      let allAddresses = this.txAddresses(tx);
      let addressesToCluster = this.txAddressesToCluster(tx);
      let addressToClusterPromises = [];
  
      for (let address of allAddresses) {
        addressToClusterPromises.push(this.getAddressClusterInfo(address));
      }
      let addressesWithClusterInfo = await Promise.all(addressToClusterPromises);
  
      let singleAddressClustersToCreate = addressesWithClusterInfo
      .filter(v => v.clusterId === undefined && !addressesToCluster.has(v.address))
      .map(v => [v.address]);
      await this.clusterAddressService.createMultipleAddressClusters(singleAddressClustersToCreate);
  
      let addressesWithClustersToCluster = addressesWithClusterInfo.filter(v => addressesToCluster.has(v.address));
  
      let clusterIds: number[] = Array.from(new Set( addressesWithClustersToCluster.filter(v => v.clusterId !== undefined).map(v => v.clusterId) )).sort();
  
      if (clusterIds.length === 0) {
      } else {
        let toCluster: number = clusterIds[0];
        let fromClusters: number[] = clusterIds.slice(1);
        if (fromClusters.length > 0) {
          console.log("merging to",toCluster, "from ", fromClusters.join(","));
          await this.clusterAddressService.mergeClusterAddresses(toCluster, ...fromClusters);
          await this.clusterBalanceService.mergeClusterTransactions(toCluster, ...fromClusters);
        }
      }
      let addressBalanceChanges = this.getTransactionAddressBalanceChanges(tx);
      let clusterBalanceChanges = await this.addressBalanceChangesToClusterBalanceChanges(addressBalanceChanges);
      await this.clusterBalanceService.saveClusterBalanceChanges(tx.txid, block.height, txindex, clusterBalanceChanges);
    }
    await this.blockService.saveBlockHash(block.height, block.hash);*/
  }

}  

class Cluster {
  constructor(
    public addresses: Set<string> = new Set(), 
    public clusterIds: Set<number> = new Set()
  ) {}

  mergeFrom(anotherCluster: Cluster) {
    anotherCluster.addresses.forEach(address => this.addresses.add(address));
    anotherCluster.clusterIds.forEach(clusterId => this.clusterIds.add(clusterId));
  }

  intersectsWith(anotherCluster: Cluster): boolean {
    for (const address of this.addresses) {
      if (anotherCluster.addresses.has(address)) return true;
    }
    for (const clusterId of this.clusterIds) {
      if (anotherCluster.clusterIds.has(clusterId)) return true;
    }
    return false;
  }

  clusterIdsSorted(): number[] {
    return Array.from(this.clusterIds).sort((a, b) => a-b);
  }

}