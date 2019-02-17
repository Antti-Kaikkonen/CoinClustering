import { ClusterBuilder } from "../misc/cluster-builder";
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
import { txAddressBalanceChanges, txAddresses } from "../utils/utils";
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
    let clusterBuilder = new ClusterBuilder(this.addressClusterTable);
    block.tx.forEach(tx => clusterBuilder.add(tx));
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


  async getClusterBalanceAndTxCount(clusterId: number): Promise<{balance: number, txCount: number}> {
    let promises = [];
    promises.push(this.clusterTransactionService.getClusterBalance(clusterId));
    promises.push(this.clusterTransactionService.getTransactionCount(clusterId));
    let res = await Promise.all(promises);
    return {balance: res[0], txCount: res[1]};
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

    let addressToClusterId: Map<string, number> = new Map();

    let clusterIdToBalanceAndTxCountPromise: Map<number, Promise<{balance: number, txCount: number}>> = new Map();
    let allAddressesResolved: Promise<void> = new Promise((resolve, reject) => {
      let addressesToReolve: number = blockAddressesArray.length;
      blockAddressesArray.forEach(address => {
        let addressToClusterIdPromise: Promise<{clusterId: number}> = this.addressClusterTable.get({address: address});
        addressToClusterIdPromise.then((res: {clusterId: number}) => {
          let clusterId = res.clusterId;
          addressToClusterId.set(address, clusterId);
          if (!clusterIdToBalanceAndTxCountPromise.has(clusterId)) {
            clusterIdToBalanceAndTxCountPromise.set(clusterId, this.getClusterBalanceAndTxCount(clusterId));
          }
          addressesToReolve--;
          if (addressesToReolve === 0) resolve();
        });
      });
    });
    await allAddressesResolved;

    let clusterIdToBalance: Map<number, number> = new Map();
    let clusterIdToOldBalance: Map<number, number> = new Map();
    let clusterIdToTxCount: Map<number, number> = new Map();
    let clusterIdToOldTxCount: Map<number, number> = new Map();

    for (let clusterId of clusterIdToBalanceAndTxCountPromise.keys()) {
      let balanceAndTxCount: {balance: number, txCount: number} = await clusterIdToBalanceAndTxCountPromise.get(clusterId);
      let txCount = balanceAndTxCount.txCount;
      let balance = balanceAndTxCount.balance;
      clusterIdToTxCount.set(clusterId, txCount);
      clusterIdToOldTxCount.set(clusterId, txCount);
      clusterIdToBalance.set(clusterId, balance);
      clusterIdToOldBalance.set(clusterId, balance);
    };
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