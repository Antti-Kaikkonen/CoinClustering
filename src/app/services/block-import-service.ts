import { ClusterBuilder } from "../misc/cluster-builder";
import { txAddressBalanceChanges, txAddresses } from "../misc/utils";
import { BlockWithTransactions } from "../models/block";
import { Cluster } from "../models/cluster";
import { AddressBalanceTable } from "../tables/address-balance-table";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { AddressTransactionTable } from "../tables/address-transaction-table";
import { BalanceToClusterTable } from "../tables/balance-to-cluster-table";
import { ClusterAddressTable } from "../tables/cluster-address-table";
import { ClusterBalanceTable } from "../tables/cluster-balance-table";
import { ClusterMergedToTable } from "../tables/cluster-merged-to-table";
import { ClusterTransactionTable } from "../tables/cluster-transaction-table";
import { LastMergedHeightTable } from "../tables/last-merged-height-table";
import { LastSavedTxHeightTable } from "../tables/last-saved-tx-height-table";
import { NextClusterIdTable } from "../tables/next-cluster-id-table";
import { AddressEncodingService } from "./address-encoding-service";
import { AddressService } from "./address-service";
import { BinaryDB } from "./binary-db";
import { ClusterAddressService } from "./cluster-address-service";
import { ClusterTransactionService } from "./cluster-transaction-service";

export class BlockImportService {

  addressClusterTable: AddressClusterTable;
  clusterMergedToTable: ClusterMergedToTable;
  nextClusterIdTable: NextClusterIdTable;
  lastMergedHeightTable: LastMergedHeightTable;
  lastSavedTxHeightTable: LastSavedTxHeightTable;
  clusterTransactionTable: ClusterTransactionTable;
  balanceToClusterTable: BalanceToClusterTable;
  clusterBalanceTable: ClusterBalanceTable;
  addressTransactionTable: AddressTransactionTable;
  addressBalanceTable: AddressBalanceTable;
  clusterAddressTable: ClusterAddressTable;

  constructor(private db: BinaryDB,
    private clusterAddressService: ClusterAddressService, 
    private clusterTransactionService: ClusterTransactionService,
    private addressService: AddressService,
    addressEncodingService: AddressEncodingService) {
      this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
      this.clusterMergedToTable = new ClusterMergedToTable(db);
      this.nextClusterIdTable = new NextClusterIdTable(db);
      this.lastMergedHeightTable = new LastMergedHeightTable(db);
      this.lastSavedTxHeightTable = new LastSavedTxHeightTable(db);
      this.clusterTransactionTable = new ClusterTransactionTable(db);
      this.balanceToClusterTable = new BalanceToClusterTable(db);
      this.clusterBalanceTable = new ClusterBalanceTable(db);
      this.addressTransactionTable = new AddressTransactionTable(db, addressEncodingService);
      this.addressBalanceTable = new AddressBalanceTable(db, addressEncodingService);
      this.clusterAddressTable = new ClusterAddressTable(db, addressEncodingService);
  }  

  lastMergedHeight: number;
  lastSavedTxHeight: number;
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

  async processClusters(clusters: Cluster[], lastBlockHeight: number) {
    let promises: Promise<any>[] = [];
    for (let cluster of clusters) {
      let clusterIds: number[] = cluster.clusterIdsSorted();
      if (clusterIds.length === 0) {
        await this.clusterAddressService.createAddressClustersOps(cluster.addresses, await this.getNextClusterId());
        this.nextClusterId++;
      } else {
        let toClusterId = clusterIds[0];
        let fromClusters = clusterIds.slice(1);
        if (fromClusters.length > 0 || cluster.addresses.length > 0) {
          console.log(lastBlockHeight.toString(),"merging to",toClusterId,"from",fromClusters.join(","));
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
    let addressToBalance: Map<string, number> = new Map();
    let addressToOldBalance: Map<string, number> = new Map();

    let clusterIdToBalancePromise: Map<number, Promise<number>> = new Map();
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
        let addressToClusterIdPromise: Promise<{clusterId: number}> = this.addressClusterTable.get({address: address});
        addressToClusterIdPromise.then((res: {clusterId: number}) => {
          let clusterId = res.clusterId;
          addressToClusterId.set(address, clusterId);
          if (!clusterIdToBalancePromise.has(clusterId)) {
            clusterIdToBalancePromise.set(clusterId, this.clusterTransactionService.getClusterBalanceDefaultZero(clusterId));
          }
          addressesToReolve--;
          if (addressesToReolve === 0) resolve();
        });
      });
    });
    await allAddressesResolved;

    let clusterIdToBalance: Map<number, number> = new Map();
    let clusterIdToOldBalance: Map<number, number> = new Map();

    for (let clusterId of clusterIdToBalancePromise.keys()) {
      let balance = await clusterIdToBalancePromise.get(clusterId);
      clusterIdToBalance.set(clusterId, balance);
      clusterIdToOldBalance.set(clusterId, balance);
    };
    for (const [txN, tx] of block.tx.entries()) {
      let addressBalanceChanges = txAddressBalanceChanges(tx);
      let clusterIdToDelta: Map<number, number> = new Map();
      for (const [address, delta] of addressBalanceChanges) {
        let addressBalance = addressToBalance.get(address);
        if (addressBalance === undefined) addressBalance = 0;
        addressBalance += delta;
        addressToBalance.set(address, addressBalance);
        await this.db.writeBatchService.push(
          this.addressTransactionTable.putOperation({address: address, height: block.height, n: txN}, {txid: tx.txid, balance: addressBalance})
        );
      //addressBalanceChanges.forEach((delta: number, address: string) => {
        if (address === undefined) throw new Error("undefined address");
        let clusterId: number = addressToClusterId.get(address);
        if (clusterId === undefined) throw new Error("undefined clusterid for address " + address);
        //TODO update address balance
        let oldDelta = clusterIdToDelta.get(clusterId);
        if (oldDelta === undefined) oldDelta = 0;
        clusterIdToDelta.set(clusterId, oldDelta+delta);
        
      };
      for (const [clusterId, delta] of clusterIdToDelta) {
        let oldBalance = clusterIdToBalance.get(clusterId);
        if (oldBalance === undefined) oldBalance = 0;
        let newBalance: number = oldBalance + delta;
        clusterIdToBalance.set(clusterId, newBalance);
        await this.db.writeBatchService.push(
          this.clusterTransactionTable.putOperation({clusterId: clusterId, height: block.height, n: txN}, {txid: tx.txid})
        );
      };
    }
    for (const [address, balance] of addressToBalance) {
      let clusterId = addressToClusterId.get(address); 
      let oldBalance = addressToOldBalance.get(address);
      if (oldBalance === undefined) oldBalance = 0;
      await this.db.writeBatchService.push(
        this.addressBalanceTable.putOperation({address: address}, {balance: balance})
      );
      if (oldBalance !== balance) {
        await this.db.writeBatchService.push(this.clusterAddressTable.putOperation({clusterId: clusterId, balance: balance, address: address}, {}))
        await this.db.writeBatchService.push(this.clusterAddressTable.delOperation({clusterId: clusterId, balance: oldBalance, address: address}));
      }
    }
    for (const [clusterId, balance] of clusterIdToBalance) {
      let oldBalance = clusterIdToOldBalance.get(clusterId);
      let balanceExists = oldBalance !== undefined;
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