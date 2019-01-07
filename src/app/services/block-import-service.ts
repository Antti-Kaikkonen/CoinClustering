import { AbstractBatch } from "abstract-leveldown";
import { BlockWithTransactions } from "../models/block";
import { Cluster } from "../models/cluster";
import { Transaction } from "../models/transaction";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { ClusterMergedToTable } from "../tables/cluster-merged-to-table";
import { LastMergedHeightTable } from "../tables/last-merged-height-table";
import { LastSavedTxHeightTable } from "../tables/last-saved-tx-height-table";
import { LastSavedTxNTable } from "../tables/last-saved-tx-n-table";
import { NextClusterIdTable } from "../tables/next-cluster-id-table";
import { JSONtoAmount } from "../utils/utils";
import { BinaryDB } from "./binary-db";
import { BlockService } from "./block-service";
import { ClusterAddressService } from "./cluster-address-service";
import { ClusterBalanceService } from "./cluster-balance-service";

export class BlockImportService {

  addressClusterTable: AddressClusterTable;
  clusterMergedToTable: ClusterMergedToTable;
  nextClusterIdTable: NextClusterIdTable;
  lastMergedHeightTable: LastMergedHeightTable;
  lastSavedTxHeightTable: LastSavedTxHeightTable;
  lastSavedTxNTable: LastSavedTxNTable;

  constructor(private db: BinaryDB,
    private clusterAddressService: ClusterAddressService, 
    private clusterBalanceService: ClusterBalanceService,
    private blockService: BlockService) {
      this.addressClusterTable = new AddressClusterTable(db);
      this.clusterMergedToTable = new ClusterMergedToTable(db);
      this.nextClusterIdTable = new NextClusterIdTable(db);
      this.lastMergedHeightTable = new LastMergedHeightTable(db);
      this.lastSavedTxHeightTable = new LastSavedTxHeightTable(db);
      this.lastSavedTxNTable = new LastSavedTxNTable(db);
  }  

  lastMergedHeight: number;
  lastSavedTxHeight: number;
  lastSavedTxN: number;
  nextClusterId: number;

  private getTransactionAddressBalanceChanges(tx: Transaction): Map<string, number> {
    let addressToDelta = new Map<string, number>();
    tx.vin.filter(vin => vin.address)
    .forEach(vin => {
      let oldBalance = addressToDelta.get(vin.address);
      if (!oldBalance) oldBalance = 0;
      addressToDelta.set(vin.address, oldBalance-JSONtoAmount(vin.value));
    }); 
    tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1)
    .forEach(vout => {
      let oldBalance = addressToDelta.get(vout.scriptPubKey.addresses[0]);
      if (!oldBalance) oldBalance = 0;
      addressToDelta.set(vout.scriptPubKey.addresses[0], oldBalance+JSONtoAmount(vout.value));
    });
    return addressToDelta;
  }
  
  /*private async addressBalanceChangesToClusterBalanceChanges2(addressToDelta: Map<string, number>, addressToClusterId: Map<string, number>): Promise<Map<string, number>> {
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
  }*/

  private async addressBalanceChangesToClusterBalanceChanges(addressToDelta: Map<string, number>): Promise<Map<number, number>> {
    let promises = [];
    let addresses = [];
    addressToDelta.forEach((delta: number, address: string) => {
      addresses.push(address);
      promises.push(this.addressClusterTable.get({address: address}));
      //promises.push(this.db.get(db_address_cluster_prefix+address));
    });
    let clusterIds = await Promise.all(promises);
    let clusterToDelta = new Map<number, number>();
    addresses.forEach((address: string, index: number) => {
      let clusterId: number = clusterIds[index].clusterId;
      if (clusterId === undefined) throw Error("Cluster missing");
      let oldBalance = clusterToDelta.get(clusterId);
      let addressDelta = addressToDelta.get(address);
      if (!oldBalance) oldBalance = 0;
      clusterToDelta.set(clusterId, oldBalance+addressDelta);
    });
    return clusterToDelta;
  }
  
  private txAddressesToCluster(tx: Transaction): Set<string> {
    let result = new Set<string>();
    if (this.isMixingTx(tx)) return result;
    tx.vin.map(vin => vin.address).filter(address => address !== undefined).forEach(address => result.add(address));
    return result;
  }
  
  private txAddresses(tx: Transaction): Set<string> {
    let result = new Set<string>();
    tx.vin.map(vin => vin.address).filter(address => address !== undefined).forEach(address => result.add(address));
    tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1 && vout.scriptPubKey.addresses[0])
    .map(vout => vout.scriptPubKey.addresses[0]).forEach(address => result.add(address));
    return result;
  }

  private isMixingTx(tx: Transaction) {
    if (tx.vin.length < 2) return false;
    if (tx.vout.length !== tx.vin.length) return false;
    let firstInput = tx.vin[0];
    if (typeof firstInput.value !== 'number') return false;
    if (!tx.vin.every(vin => vin.value === firstInput.value)) return false;
    if (!tx.vout.every(vout => vout.value === firstInput.value)) return false;
    return true;
  }
  
  private async getAddressClusterInfo(address: string): Promise<{address: string, clusterId?: number}> {
    try {
      let cluster = await this.addressClusterTable.get({address: address});
      return {address: address, clusterId: cluster.clusterId};
    } catch(err) {
      return {address: address};
    }  
      /*this.db.get(db_address_cluster_prefix+address, (error, clusterId: string) => {
        if (clusterId !== undefined) {
          resolve( { address: address, clusterId: Number(clusterId) });
        } else {
          resolve({address: address});
        }
      });*/
  }

  async processClusters(clusters: Cluster[], lastBlockHeight: number) {
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    let promises: Promise<any>[] = [];
    for (let cluster of clusters) {
      let clusterIds: number[] = cluster.clusterIdsSorted();
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
          if (fromClusters.length > 0 && await this.getLastSavedTxHeight() > -1) promises.push(this.clusterBalanceService.mergeClusterTransactionsOps(toClusterId, ...fromClusters));
          fromClusters.forEach(fromClusterId => {
            ops.push(
              this.clusterMergedToTable.putOperation({fromClusterId: fromClusterId}, {toClusterId: toClusterId})
            );
            /*ops.push({
              type:"put",
              key:db_cluster_merged_to+fromClusterId,
              value:toClusterId
            })*/
          });
        }
      }  
    }  
    let v = await Promise.all(promises);
    v.forEach(opGroup => opGroup.forEach(op => ops.push(op)));

    ops.push(
      this.lastMergedHeightTable.putOperation(undefined, {height:lastBlockHeight})
    );
    /*ops.push({
      type:"put",
      key:db_last_merged_block_height,
      value:lastBlockHeight
    });*/
    ops.push(
      this.nextClusterIdTable.putOperation(undefined, {nextClusterId: await this.getNextClusterId()})
    );
    /*ops.push({
      type:"put",
      key:db_next_cluster_id,
      value: await this.getNextClusterId()
    });*/

    if (ops.length > 1000) console.log("ops.length: ", ops.length);
    await this.db.batchBinary(ops);
  }


  private async computeClusters(block: BlockWithTransactions): Promise<Cluster[]> {
    let txs = block.tx;

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

    this.mergeIntersectingClusters(newClusters);

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
    //console.log("newClusters", newClusters);
    return newClusters;
  }

  private mergeIntersectingClusters(clusters: Cluster[]): void {
    for (let i = 0; i < clusters.length; i++) {
      let clusterA = clusters[i];
      let mergedToClusterA;
      do {
        mergedToClusterA = false;
        for (let ii = i+1; ii < clusters.length; ii++) {
          let clusterB = clusters[ii];
          if (clusterA.intersectsWith(clusterB)) {
            clusterA.mergeFrom(clusterB);
            clusters.splice(ii, 1);
            mergedToClusterA = true;
            break;
          }  
        }
      } while (mergedToClusterA);
    }
  }

  async blockMerging(block: BlockWithTransactions) {
    if (block.height <= await this.getLastMergedHeight()) return;
    await this.processClusters(await this.computeClusters(block), block.height);
    this.lastMergedHeight = block.height;
  }

  async saveBlockTransactions(block: BlockWithTransactions) {
    if (block.height <= await this.getLastSavedTxHeight()) return;//already saved
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
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    ops.push(
      this.lastSavedTxHeightTable.putOperation(undefined, {height: block.height})
    );
    /*ops.push({
      type: "put",
      key: db_last_saved_tx_height,
      value: block.height
    });*/
    ops.push(
      this.lastSavedTxNTable.delOperation(undefined)
    )
    /*ops.push({
      type: "del",
      key: db_last_saved_tx_n
    });*/
    await this.db.batchBinary(ops);
    this.lastSavedTxN = -1;
  }

  async getLastMergedHeight(): Promise<number> {
    if (this.lastMergedHeight === undefined) {
      try {
        this.lastMergedHeight = (await this.lastMergedHeightTable.get(undefined)).height;
        //this.lastMergedHeight = Number(await this.db.get(db_last_merged_block_height));
      } catch (err) {
        this.lastMergedHeight = -1;
      }
    }
    return this.lastMergedHeight;
  }

  async getLastSavedTxHeight(): Promise<number> {
    if (this.lastSavedTxHeight === undefined) {
      try {
        this.lastSavedTxHeight = (await this.lastSavedTxHeightTable.get(undefined)).height;
        //this.lastSavedTxHeight = Number(await this.db.get(db_last_saved_tx_height));
      } catch (err) {
        this.lastSavedTxHeight = -1;
      }
    }
    return this.lastSavedTxHeight;
  }

  private async getLastSavedTxN(): Promise<number> {
    if (this.lastSavedTxN === undefined) {
      try {
        this.lastSavedTxN = (await this.lastSavedTxNTable.get(undefined)).n;
        //this.lastSavedTxN = Number(await this.db.get(db_last_saved_tx_n));
      } catch (err) {
        this.lastSavedTxN = -1;
      }
    }
    return this.lastSavedTxN;
  }

  private async getNextClusterId(): Promise<number> {
    if (this.nextClusterId === undefined) {
      try {
        this.nextClusterId = (await this.nextClusterIdTable.get(undefined)).nextClusterId;
        //this.nextClusterId = Number(await this.db.get(db_next_cluster_id));
      } catch(err) {
        this.nextClusterId = 0;
      }
    }
    return this.nextClusterId;
  }

  async saveBlock(block: BlockWithTransactions) {
    console.log("saving block ");
    if (block.height%1000 === 0) {
      console.log(block.height);
    }  
    if (block.height > await this.getLastMergedHeight()) {
      await this.blockMerging(block);
      this.lastMergedHeight = block.height;
    }
    if (block.height >= await this.getLastSavedTxHeight()) {
      await this.saveBlockTransactions(block);
    }
  }

}  