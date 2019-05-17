import { injectable } from "inversify";
import { ClusterBuilder } from "../misc/cluster-builder";
import { txAddressBalanceChanges, txAddresses } from "../misc/utils";
import { BlockWithTransactions } from "../models/block";
import { Cluster } from "../models/cluster";
import { ClusterId } from "../models/clusterid";
import { AddressBalanceTable } from "../tables/address-balance-table";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { AddressTransactionTable } from "../tables/address-transaction-table";
import { BalanceToClusterTable } from "../tables/balance-to-cluster-table";
import { ClusterAddressTable } from "../tables/cluster-address-table";
import { ClusterBalanceTable } from "../tables/cluster-balance-table";
import { ClusterMergedToTable } from "../tables/cluster-merged-to-table";
import { ClusterTransactionCountTable } from "../tables/cluster-transaction-count-table";
import { ClusterTransactionTable } from "../tables/cluster-transaction-table";
import { LastMergedHeightTable } from "../tables/last-merged-height-table";
import { LastSavedTxHeightTable } from "../tables/last-saved-tx-height-table";
import { AddressService } from "./address-service";
import { BinaryDB } from "./binary-db";
import { ClusterAddressService } from "./cluster-address-service";
import { ClusterTransactionService } from "./cluster-transaction-service";

@injectable()
export class BlockImportService {

  constructor(private db: BinaryDB,
    private clusterAddressService: ClusterAddressService, 
    private clusterTransactionService: ClusterTransactionService,
    private addressService: AddressService,
    private addressClusterTable: AddressClusterTable,
    private clusterMergedToTable: ClusterMergedToTable,
    private lastMergedHeightTable: LastMergedHeightTable,
    private lastSavedTxHeightTable: LastSavedTxHeightTable,
    private clusterTransactionTable: ClusterTransactionTable,
    private balanceToClusterTable: BalanceToClusterTable,
    private clusterBalanceTable: ClusterBalanceTable,
    private addressTransactionTable: AddressTransactionTable,
    private addressBalanceTable: AddressBalanceTable,
    private clusterAddressTable: ClusterAddressTable,
    private clusterTransactionCountTable: ClusterTransactionCountTable
  ) {
  }  

  lastMergedHeight: number;
  lastSavedTxHeight: number;

  async processClusters(clusters: Cluster[], lastBlockHeight: number) {
    let promises: Promise<any>[] = [];
    for (let cluster of clusters) {
      let clusterIds: ClusterId[] = cluster.clusterIdsSorted();
      if (clusterIds.length === 0) {
        await this.clusterAddressService.createAddressClustersOps(cluster.addresses, cluster.lowestClusterId);
      } else {
        let toClusterId = clusterIds[0];
        let fromClusters = clusterIds.slice(1);
        if (fromClusters.length > 0 || cluster.addresses.length > 0) {
          console.log(lastBlockHeight.toString(),"merging to",toClusterId,"from",fromClusters);
          promises.push(this.clusterAddressService.mergeClusterAddressesOps(toClusterId, fromClusters, cluster.addresses));
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
    await this.db.writeBatchService.commit();
  }

  private async computeClusters(block: BlockWithTransactions): Promise<Cluster[]> {
    let clusterBuilder = new ClusterBuilder(this.addressClusterTable);
    block.tx.forEach((tx, txN) => clusterBuilder.add(tx, block.height, txN));
    return clusterBuilder.build();
  }

  processClustersTime = 0;
  computeClustersTime = 0;

  async blockMerging(block: BlockWithTransactions) {
    if (block.height <= await this.getLastMergedHeight()) return;
    let t1 = new Date().getTime()
    let clusters = await this.computeClusters(block);
    let t2 = new Date().getTime();
    await this.processClusters(clusters, block.height);
    let t3 = new Date().getTime();
    this.computeClustersTime += t2-t1;
    this.processClustersTime += t3-t2;
    console.log("compute:",this.computeClustersTime+", process:",this.processClustersTime);
    this.lastMergedHeight = block.height;
  }

  async saveBlockTransactionsAsync(block: BlockWithTransactions): Promise<void> {
    console.log(block.height, "saving transactions");
    if (block.height <= await this.getLastSavedTxHeight()) {
      console.log("already saved");
      return;
    }  
    let blockAddresses: Set<string> = new Set();
    for (let tx of block.tx) {
      let txAddr = txAddresses(tx);
      txAddr.forEach(txAddress => blockAddresses.add(txAddress));
    }
    let blockAddressesArray = Array.from(blockAddresses);

    let addressToClusterId: Map<string, string> = new Map();
    let addressToBalance: Map<string, number> = new Map();
    let addressToOldBalance: Map<string, number> = new Map();

    let clusterIdToBalancePromise: Map<string, Promise<number>> = new Map();
    let clusterIdToTxCountPromsie: Map<string, Promise<number>> = new Map();
    let allAddressesResolved: Promise<void> = new Promise((resolve, reject) => {
      let addressesToReolve: number = blockAddressesArray.length*2;
      if (addressesToReolve === 0) resolve();
      blockAddressesArray.forEach(address => {
        this.addressService.getAddressBalanceDefaultUndefined(address).then(balance => {
          addressToBalance.set(address, balance);
          addressToOldBalance.set(address, balance);
          addressesToReolve--;
          if (addressesToReolve === 0) resolve();
        });
        let addressToClusterIdPromise: Promise<{clusterId: ClusterId}> = this.addressClusterTable.get({address: address});
        addressToClusterIdPromise.then((res: {clusterId: ClusterId}) => {
          let clusterId = res.clusterId;
          addressToClusterId.set(address, clusterId.toString());
          if (!clusterIdToBalancePromise.has(clusterId.toString())) {//TODO fix...
            clusterIdToBalancePromise.set(clusterId.toString(), this.clusterTransactionService.getClusterBalanceDefaultZero(clusterId));
            clusterIdToTxCountPromsie.set(clusterId.toString(), this.clusterTransactionService.getClusterTransactionCountDefaultZero(clusterId));
          }
          addressesToReolve--;
          if (addressesToReolve === 0) resolve();
        });
      });
    });
    await allAddressesResolved;

    let clusterIdToBalance: Map<string, number> = new Map();
    let clusterIdToOldBalance: Map<string, number> = new Map();
    let clusterIdToTxCount: Map<string, number> = new Map();

    for (let clusterId of clusterIdToBalancePromise.keys()) {
      let balance = await clusterIdToBalancePromise.get(clusterId);
      clusterIdToBalance.set(clusterId, balance);
      clusterIdToOldBalance.set(clusterId, balance);
      let txCount = await clusterIdToTxCountPromsie.get(clusterId);
      clusterIdToTxCount.set(clusterId, txCount);
    };
    for (const [txN, tx] of block.tx.entries()) {
      let addressBalanceChanges = txAddressBalanceChanges(tx);
      let clusterIdToDelta: Map<string, number> = new Map();
      for (const [address, delta] of addressBalanceChanges) {
        let addressBalance = addressToBalance.get(address);
        if (addressBalance === undefined) addressBalance = 0;
        addressBalance += delta;
        addressToBalance.set(address, addressBalance);
        await this.db.writeBatchService.push(
          this.addressTransactionTable.putOperation({address: address, height: block.height, n: txN}, {txid: tx.txid, balance: addressBalance})
        );
        if (address === undefined) throw new Error("undefined address");
        let clusterId: string = addressToClusterId.get(address);
        if (clusterId === undefined) throw new Error("undefined clusterid for address " + address);
        let oldDelta = clusterIdToDelta.get(clusterId);
        if (oldDelta === undefined) oldDelta = 0;
        clusterIdToDelta.set(clusterId, oldDelta+delta);
        
      };
      for (const [clusterId, delta] of clusterIdToDelta) {
        let txCount = clusterIdToTxCount.get(clusterId);
        txCount++;
        clusterIdToTxCount.set(clusterId, txCount);
        let oldBalance = clusterIdToBalance.get(clusterId);
        if (oldBalance === undefined) oldBalance = 0;
        let newBalance: number = oldBalance + delta;
        clusterIdToBalance.set(clusterId, newBalance);
        await this.db.writeBatchService.push(
          this.clusterTransactionTable.putOperation({clusterId: ClusterId.fromString(clusterId), height: block.height, n: txN}, {txid: tx.txid, balanceChange: delta})
        );
      };
    }
    for (const [clusterId, txCount] of clusterIdToTxCount) {
      await this.db.writeBatchService.push(this.clusterTransactionCountTable.putOperation({clusterId: ClusterId.fromString(clusterId)}, {transactionCount: txCount}));
    };
    for (const [address, balance] of addressToBalance) {
      let clusterId = addressToClusterId.get(address); 
      let oldBalance = addressToOldBalance.get(address);
      if (oldBalance === undefined) oldBalance = 0;
      await this.db.writeBatchService.push(
        this.addressBalanceTable.putOperation({address: address}, {balance: balance})
      );
      if (oldBalance !== balance) {
        await this.db.writeBatchService.push(this.clusterAddressTable.putOperation({clusterId: ClusterId.fromString(clusterId), balance: balance, address: address}, {}))
        await this.db.writeBatchService.push(this.clusterAddressTable.delOperation({clusterId: ClusterId.fromString(clusterId), balance: oldBalance, address: address}));
      }
    }
    for (const [clusterId, balance] of clusterIdToBalance) {
      let oldBalance = clusterIdToOldBalance.get(clusterId);
      let balanceExists = oldBalance !== undefined;
      await this.db.writeBatchService.push(this.clusterBalanceTable.putOperation({clusterId: ClusterId.fromString(clusterId)}, {balance: balance}));

      if (!balanceExists || oldBalance !== balance) {
        await this.db.writeBatchService.push(this.balanceToClusterTable.putOperation({balance: balance, clusterId: ClusterId.fromString(clusterId)}, {}));
        if (balanceExists) 
          await this.db.writeBatchService.push(this.balanceToClusterTable.delOperation({balance: oldBalance, clusterId: ClusterId.fromString(clusterId)}));
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