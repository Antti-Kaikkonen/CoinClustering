import { BlockWithTransactions } from "../models/block";
import { Cluster } from "../models/cluster";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { BalanceToClusterTable } from "../tables/balance-to-cluster-table";
import { ClusterBalanceTable } from "../tables/cluster-balance-table";
import { ClusterMergedToTable } from "../tables/cluster-merged-to-table";
import { ClusterTransactionCountTable } from "../tables/cluster-transaction-count-table";
import { ClusterTransactionTable } from "../tables/cluster-transaction-table";
import { LastMergedHeightTable } from "../tables/last-merged-height-table";
import { LastSavedTxHeightTable } from "../tables/last-saved-tx-height-table";
import { LastSavedTxNTable } from "../tables/last-saved-tx-n-table";
import { NextClusterIdTable } from "../tables/next-cluster-id-table";
import { txAddressBalanceChanges, txAddresses, txAddressesToCluster } from "../utils/utils";
import { AddressEncodingService } from "./address-encoding-service";
import { BinaryDB } from "./binary-db";
import { ClusterAddressService } from "./cluster-address-service";
import { ClusterTransactionService } from "./cluster-transaction-service";

export class BlockImportService {

  addressClusterTable: AddressClusterTable;
  clusterMergedToTable: ClusterMergedToTable;
  nextClusterIdTable: NextClusterIdTable;
  lastMergedHeightTable: LastMergedHeightTable;
  lastSavedTxHeightTable: LastSavedTxHeightTable;
  lastSavedTxNTable: LastSavedTxNTable;
  clusterTransactionTable: ClusterTransactionTable;
  balanceToClusterTable: BalanceToClusterTable;
  clusterBalanceCountTable: ClusterTransactionCountTable;
  clusterBalanceTable: ClusterBalanceTable;

  constructor(private db: BinaryDB,
    private clusterAddressService: ClusterAddressService, 
    private clusterTransactionService: ClusterTransactionService,
    addressEncodingService: AddressEncodingService) {
      this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
      this.clusterMergedToTable = new ClusterMergedToTable(db);
      this.nextClusterIdTable = new NextClusterIdTable(db);
      this.lastMergedHeightTable = new LastMergedHeightTable(db);
      this.lastSavedTxHeightTable = new LastSavedTxHeightTable(db);
      this.lastSavedTxNTable = new LastSavedTxNTable(db);
      this.clusterTransactionTable = new ClusterTransactionTable(db);
      this.balanceToClusterTable = new BalanceToClusterTable(db);
      this.clusterBalanceCountTable = new ClusterTransactionCountTable(db);
      this.clusterBalanceTable = new ClusterBalanceTable(db);
  }  

  lastMergedHeight: number;
  lastSavedTxHeight: number;
  lastSavedTxN: number;
  nextClusterId: number;


  private async addressBalanceChangesToClusterBalanceChanges(addressToDelta: Map<string, number>): Promise<Map<number, number>> {
    let promises = [];
    let addresses = [];
    addressToDelta.forEach((delta: number, address: string) => {
      addresses.push(address);
      promises.push(this.addressClusterTable.get({address: address}));
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
  

  
  private async getAddressClusterInfo(address: string): Promise<{address: string, clusterId?: number}> {
    try {
      let cluster = await this.addressClusterTable.get({address: address});
      return {address: address, clusterId: cluster.clusterId};
    } catch(err) {
      return {address: address};
    }  
  }

  async processClusters(clusters: Cluster[], lastBlockHeight: number) {
    let promises: Promise<any>[] = [];
    for (let cluster of clusters) {
      let clusterIds: number[] = cluster.clusterIdsSorted();
      let clusterAddresses: string[] = Array.from(cluster.addresses);
      if (clusterIds.length === 0) {
        await this.clusterAddressService.createAddressClustersOps(clusterAddresses, await this.getNextClusterId());
        this.nextClusterId++;
      } else {
        let toClusterId = clusterIds[0];
        let fromClusters = clusterIds.slice(1);
        if (fromClusters.length > 0 || clusterAddresses.length > 0) {
          console.log(lastBlockHeight.toString(),"merging to",toClusterId,"from",fromClusters.join(","));
          promises.push(this.clusterAddressService.mergeClusterAddressesOps(toClusterId, fromClusters, clusterAddresses));
          if (fromClusters.length > 0 && await this.getLastSavedTxHeight() > -1) promises.push(this.clusterTransactionService.mergeClusterTransactionsOps(toClusterId, ...fromClusters));
          for (const fromClusterId of fromClusters) {  
            await this.db.writeBatchService.push(
              this.clusterMergedToTable.putOperation({fromClusterId: fromClusterId}, {toClusterId: toClusterId})
            );
          };
        }
      }  
    }
    await Promise.all(promises);

    await this.db.writeBatchService.push(
      this.lastMergedHeightTable.putOperation(undefined, {height:lastBlockHeight})
    );
    await this.db.writeBatchService.push(
      this.nextClusterIdTable.putOperation(undefined, {nextClusterId: await this.getNextClusterId()})
    );

    await this.db.writeBatchService.commit();
  }


  private async computeClusters(block: BlockWithTransactions): Promise<Cluster[]> {
    let txs = block.tx;

    let allAddresses: Set<string> = new Set();
    let txToAddress: Map<number, Set<string>> = new Map();
    let txToAddressesNotToCluster: Map<number, Set<string>> = new Map();
    let txToAddressesToCluster: Map<number, Set<string>> = new Map();
    for (const [index, tx] of txs.entries()) {
      let txAddr = txAddresses(tx);
      if (txAddr.size > 0) txToAddress.set(index, new Set());
      txAddr.forEach(address => {
        allAddresses.add(address);
        txToAddress.get(index).add(address);
      });

      let addressesToCluster = txAddressesToCluster(tx);
      if (addressesToCluster.size > 0) txToAddressesToCluster.set(index, new Set());
      addressesToCluster.forEach(address => {
        txToAddressesToCluster.get(index).add(address);
      });
      
      txAddr.forEach(address => {
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
        if (addressToClusterId.has(address)) return;
        let clusterContainingAddress = newClusters.find(cluster => cluster.addresses.has(address));
        if (clusterContainingAddress !== undefined) {
        } else {
          newClusters.push(new Cluster(new Set([address])));
        }
      });  
    }
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


  async saveBlockTransactionsAsync(block: BlockWithTransactions): Promise<void> {
    console.log(block.height, "saving transactions");
    if (block.height <= await this.getLastSavedTxHeight()) {
      console.log("already saved");
      return;// [];//already saved
    }  
    let blockAddresses: Set<string> = new Set();
    for (let tx of block.tx) {
      let txAddr = txAddresses(tx);
      txAddr.forEach(txAddress => blockAddresses.add(txAddress));
    }
    let blockAddressesArray = Array.from(blockAddresses);
    let blockClusterPromises: Promise<{clusterId: number}>[] = [];
    
    blockAddressesArray.forEach(address => {
      let clusterIdPromise: Promise<{clusterId: number}> = this.addressClusterTable.get({address: address});
      blockClusterPromises.push(clusterIdPromise);
    });


    let clusterIds: number[] = (await Promise.all(blockClusterPromises)).map(clusterId => clusterId.clusterId);
    let addressToClusterId: Map<string, number> = new Map();
    clusterIds.forEach((clusterId: number, index: number) => {
      if (clusterId === undefined) throw new Error("undefined clusterid 1");
      let address = blockAddressesArray[index];
      addressToClusterId.set(address, clusterId);
    });
    let clusterTransactionCountPromises: Promise<number>[] = [];
    let clusterBalancePromises: Promise<number>[] = [];
    clusterIds.forEach((clusterId: number) => {
      clusterBalancePromises.push(this.clusterTransactionService.getClusterBalance(clusterId));
      clusterTransactionCountPromises.push(this.clusterTransactionService.getTransactionCount(clusterId));
    });
    let clusterTransactionCounts = await Promise.all(clusterTransactionCountPromises);
    let clusterBalances = await Promise.all(clusterBalancePromises);
    let clusterIdToBalance: Map<number, number> = new Map();
    let clusterIdToOldBalance: Map<number, number> = new Map();
    let clusterIdToTxCount: Map<number, number> = new Map();
    let clusterIdToOldTxCount: Map<number, number> = new Map();
    clusterIds.forEach((clusterId: number, index: number) => {
      let txCount = clusterTransactionCounts[index];
      let balance = clusterBalances[index];
      clusterIdToTxCount.set(clusterId, txCount);
      clusterIdToOldTxCount.set(clusterId, txCount);
      clusterIdToBalance.set(clusterId, balance);
      clusterIdToOldBalance.set(clusterId, balance);
    });
    for (const [txN, tx] of block.tx.entries()) {
      let addressBalanceChanges = txAddressBalanceChanges(tx);
      let clusterIdToDelta: Map<number, number> = new Map();
      addressBalanceChanges.forEach((delta: number, address: string) => {
        if (address === undefined) throw new Error("undefined address");
        let clusterId: number = addressToClusterId.get(address);
        if (clusterId === undefined) throw new Error("undefined clusterid for address " + address);
        let oldDelta = clusterIdToDelta.get(clusterId);
        if (oldDelta === undefined) oldDelta = 0;
        clusterIdToDelta.set(clusterId, oldDelta+delta);
      });
      for (const [clusterId, delta] of clusterIdToDelta) {
        let txCount = clusterIdToTxCount.get(clusterId);
        txCount++;
        clusterIdToTxCount.set(clusterId, txCount);
        let oldBalance = clusterIdToBalance.get(clusterId);
        let newBalance: number = oldBalance + delta;
        clusterIdToBalance.set(clusterId, newBalance);
        await this.db.writeBatchService.push(
          this.clusterTransactionTable.putOperation({clusterId: clusterId, height: block.height, n: txN}, {txid: tx.txid, balanceDelta: delta})
        );
      };
    }
    for (const [clusterId, txCount] of clusterIdToTxCount) {
      await this.db.writeBatchService.push(this.clusterBalanceCountTable.putOperation({clusterId: clusterId}, {balanceCount: txCount}));
    };
    for (const [clusterId, balance] of clusterIdToBalance) {
      let oldBalance = clusterIdToOldBalance.get(clusterId);
      let balanceExists = clusterIdToOldTxCount.get(clusterId) > 0;
      await this.db.writeBatchService.push(this.clusterBalanceTable.putOperation({clusterId: clusterId}, {balance: balance}));
      if (!balanceExists || oldBalance !== balance) {
        await this.db.writeBatchService.push(this.balanceToClusterTable.putOperation({balance: balance, clusterId: clusterId}, {}));
        if (balanceExists) 
          await this.db.writeBatchService.push(this.balanceToClusterTable.delOperation({balance: oldBalance, clusterId: clusterId}));
      }
    };
    await this.db.writeBatchService.push(
      this.lastSavedTxHeightTable.putOperation(undefined, {height: block.height})
    );
    
    this.lastSavedTxHeight = block.height;
    await this.db.writeBatchService.commit();
  }  

  async getLastMergedHeight(): Promise<number> {
    if (this.lastMergedHeight === undefined) {
      try {
        this.lastMergedHeight = (await this.lastMergedHeightTable.get(undefined)).height;
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
      await this.saveBlockTransactionsAsync(block);
    }
  }

}  