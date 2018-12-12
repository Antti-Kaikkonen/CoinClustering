import RpcClient from "bitcoind-rpc";
import { LevelUp } from "levelup";
import { BlockService } from "./block-service";
import { ClusterAddressService } from "./cluster-address-service";
import { ClusterBalanceService } from "./cluster-balance-service";
import { db_address_cluster_prefix } from "./db-constants";

export class BlockImportService {

  constructor(private db: LevelUp, 
    private rpc: RpcClient, 
    private clusterAddressService: ClusterAddressService, 
    private clusterBalanceService: ClusterBalanceService,
    private blockService: BlockService) {

  }  

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
    return new Promise<{address: string, cluster?: {id: number}}>((resolve, reject) => {
      this.db.get(db_address_cluster_prefix+address, (error, clusterId: string) => {
        if (clusterId !== undefined) {
          resolve( { address: address, cluster: { id: Number(clusterId) }});
        } else {
          resolve({address: address});
        }
      });
    });
  }

  async saveBlock(block) {
    if (block.height%1000 === 0) console.log(block.height);
    //let rawtxs = await getRawTransactions(block.tx);
    let txs = block.tx;//await decodeRawTransactions(rawtxs)
    //if (true) return;
    for (const [txindex, tx] of txs.entries()) {
      let allAddresses = this.txAddresses(tx);
      let addressesToCluster = this.txAddressesToCluster(tx);
      let addressesNotToCluster = new Set([...allAddresses].filter(x => !addressesToCluster.has(x)));
  
      let addressToClusterPromises = [];
  
      for (let address of allAddresses) {
        addressToClusterPromises.push(this.getAddressClusterInfo(address));
      }
      let addressesWithClusterInfo = await Promise.all(addressToClusterPromises);
  
      let singleAddressClustersToCreate = addressesWithClusterInfo
      .filter(v => v.cluster === undefined && addressesNotToCluster.has(v.address))
      .map(v => [v.address]);
      await this.clusterAddressService.createMultipleAddressClusters(singleAddressClustersToCreate);
  
      let addressesWithClustersToCluster = addressesWithClusterInfo.filter(v => addressesToCluster.has(v.address));
  
      let clusterIds: number[] = Array.from(new Set( addressesWithClustersToCluster.filter(v => v.cluster !== undefined).map(v => v.cluster.id) ));
  
      let nonClusterAddresses = addressesWithClustersToCluster.filter(v => v.cluster === undefined).map(v => v.address);
  
      if (clusterIds.length === 0) {
        if (nonClusterAddresses.length > 0) {
          console.log("creating cluster with "+nonClusterAddresses.length+" addresses");
          await this.clusterAddressService.createClusterWithAddresses(nonClusterAddresses);
        }  
      } else {
        let toCluster: number = Math.min(...clusterIds);
        let fromClusters: number[] = clusterIds.filter(clusterId => clusterId !== toCluster);
        if (fromClusters.length > 0) console.log("merging to",toCluster, "from ", fromClusters.join(","));
        if (fromClusters.length > 0) {
          await this.clusterAddressService.mergeClusterAddresses(toCluster, ...fromClusters);
          await this.clusterBalanceService.mergeClusterTransactions(toCluster, ...fromClusters);
        }
  
        if (nonClusterAddresses.length > 0) {
          console.log("adding addresses", nonClusterAddresses, "to cluster", toCluster);
          await this.clusterAddressService.addAddressesToCluster(nonClusterAddresses, toCluster);
        }   
      }
      let addressBalanceChanges = this.getTransactionAddressBalanceChanges(tx);
      let clusterBalanceChanges = await this.addressBalanceChangesToClusterBalanceChanges(addressBalanceChanges);
      await this.clusterBalanceService.saveClusterBalanceChanges(tx.txid, block.height, txindex, clusterBalanceChanges);
    }
    await this.blockService.saveBlockHash(block.height, block.hash);
  }

}  