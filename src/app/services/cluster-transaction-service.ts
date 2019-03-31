import { injectable } from 'inversify';
import { Writable } from 'stream';
import { ClusterTransaction } from '../models/cluster-transaction';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterBalanceTable } from '../tables/cluster-balance-table';
import { ClusterTransactionCountTable } from '../tables/cluster-transaction-count-table';
import { ClusterTransactionTable } from '../tables/cluster-transaction-table';
import { BinaryDB } from './binary-db';

@injectable()
export class ClusterTransactionService {

  constructor(private db: BinaryDB,
    private clusterTransactionTable: ClusterTransactionTable,
    private balanceToClusterTable: BalanceToClusterTable,
    private clusterBalanceTable: ClusterBalanceTable,
    private clusterTransactionCountTable: ClusterTransactionCountTable
  ) {}  

  async getClusterTransactionCountDefaultZero(clusterId: number): Promise<number> {
    try {
      return (await this.clusterTransactionCountTable.get({clusterId: clusterId})).transactionCount;
    } catch (err) {
      if (err.notFound)
        return 0;
      else 
        throw err;
    }
  }

  async getClusterBalanceDefaultUndefined(clusterId: number): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      this.clusterBalanceTable.get({clusterId: clusterId}).then((res: {balance: number}) => {
        resolve(res.balance);
      },
      (error) => {
        if (error.notFound) 
          resolve(undefined)
        else
          reject(error);
      });
    });
  }

  async getClusterBalanceDefaultZero(clusterId: number): Promise<number> {
    let res = await this.getClusterBalanceDefaultUndefined(clusterId);
    if (res === undefined) res = 0;
    return res;
  }

  async getFirstClusterTransaction(clusterId: number): Promise<ClusterTransaction> {
    return new Promise<ClusterTransaction>((resolve, reject) => {
      let transaction;
      this.clusterTransactionTable.createReadStream({
        gte: {clusterId: clusterId},
        lt: {clusterId: clusterId+1},
        limit: 1
      }).on("data", function(data) {
        transaction = new ClusterTransaction(
          data.value.txid,
          data.key.height,
          data.key.n
        );
      }).on('error', function (err) {
        reject(err);
      })
      .on('finish', function () {
        resolve(transaction);
      });
    });
  }

  async getLastClusterTransaction(clusterId: number): Promise<ClusterTransaction> {
    return new Promise<ClusterTransaction>((resolve, reject) => {
      let transaction;
      this.clusterTransactionTable.createReadStream({
        gte: {clusterId: clusterId},
        lt: {clusterId: clusterId+1},
        limit: 1,
        reverse: true
      }).on("data", function(data) {
        transaction = new ClusterTransaction(
          data.value.txid,
          data.key.height,
          data.key.n
        );
      })
      .on('error', function (err) {
        reject(err);
      })
      .on('finish', function () {
        resolve(transaction);
      });
    });
  }

  async getClusterTransactionDefaultUndefined(clusterId: number, height: number, n: number): Promise<ClusterTransaction> {
    try {
      let value = await this.clusterTransactionTable.get({clusterId: clusterId, height: height, n: n});
      let cb = new ClusterTransaction(value.txid, height, n, value.balanceChange);
      return cb;
    } catch(err) {
      if (err.notFound) {
        return undefined;
      } else {
        throw err;
      }
    }
  }

  async getClusterTransactions(clusterId: number): Promise<ClusterTransaction[]> {
    return new Promise<ClusterTransaction[]>((resolve, reject) => {
      let transactions = [];
      this.clusterTransactionTable.createReadStream({
        gte: {clusterId: clusterId},
        lt: {clusterId: clusterId+1}
      }).on("data", function(data) {
        let cb = new ClusterTransaction(
          data.value.txid,
          data.key.height,
          data.key.n
        );
        transactions.push(
          cb
        );
      })
      .on('error', function (err) {
        reject(err);
      })
      .on('finish', function () {
        resolve(transactions);
      });
    });
  }

  async mergeClusterTransactionsOps(toCluster: number, ...fromClusters: number[]): Promise<void> {
    if (fromClusters.length === 0) return;
    let toClusterBalancePromsie = this.getClusterBalanceDefaultZero(toCluster);
    let toClusterTransactionCountPromise = this.getClusterTransactionCountDefaultZero(toCluster);
    let fromClusterBalanceSum: number = 0;
    let txIdToOldTransationPromise: Map<string, Promise<ClusterTransaction>> = new Map();
    let txidToTransactionToMerge: Map<string, ClusterTransaction> = new Map();
    let allClustersMerged = new Promise((resolve, reject) => {
      let clustersToMerge: number = fromClusters.length;
      if (clustersToMerge === 0) resolve();
      fromClusters.forEach(fromCluster => {
        let clusterBalancePromise = this.getClusterBalanceDefaultZero(fromCluster);
        let clusterTransactionCount = 0;

        let txMerger = new Writable({
          objectMode: true,
          write: async (data: {key: {height: number, n: number}, value: {txid: string, balanceChange: number}}, encoding, callback) => {
            clusterTransactionCount++;
            let tx: ClusterTransaction = new ClusterTransaction(
              data.value.txid,
              data.key.height,
              data.key.n,
              data.value.balanceChange
            );
            if (!txIdToOldTransationPromise.has(tx.txid)) {
              txIdToOldTransationPromise.set(tx.txid, this.getClusterTransactionDefaultUndefined(toCluster, tx.height, tx.n));
            }
            let txToMerge = txidToTransactionToMerge.get(tx.txid)
            if (txToMerge) {
              txToMerge.balanceChange += tx.balanceChange;
            } else {
              txidToTransactionToMerge.set(tx.txid, tx);
            }
            await this.db.writeBatchService.push(
              this.clusterTransactionTable.delOperation({clusterId: fromCluster, height: tx.height, n: tx.n})
            );
            callback(null);
          }
        });

        this.clusterTransactionTable.createReadStream({
          gte: {clusterId: fromCluster},
          lt: {clusterId: fromCluster+1}
        }).pipe(txMerger);

        txMerger.on('finish', async () => {
          await this.db.writeBatchService.push(
            this.clusterBalanceTable.delOperation({clusterId: fromCluster})
          );
          await this.db.writeBatchService.push(
            this.clusterTransactionCountTable.delOperation({clusterId: fromCluster})
          );
          let clusterBalance = await clusterBalancePromise;
          fromClusterBalanceSum += clusterBalance;
          if (clusterTransactionCount > 0) {
            await this.db.writeBatchService.push(
              this.balanceToClusterTable.delOperation({balance: clusterBalance, clusterId: fromCluster})
            );
          }
          clustersToMerge--;
          if (clustersToMerge === 0) resolve();
        });

      });
    });
    await allClustersMerged;
    let newTxCount = 0;
    for (const promise of txIdToOldTransationPromise.values()) {
      let tx = await promise;
      if (tx) {
        let txToMerge = txidToTransactionToMerge.get(tx.txid);
        txToMerge.balanceChange += tx.balanceChange;
      } else {
        newTxCount++;
      }
    }  
    for (const txToMerge of txidToTransactionToMerge.values()) {
      await this.db.writeBatchService.push(this.clusterTransactionTable.putOperation({clusterId: toCluster, height: txToMerge.height, n: txToMerge.n}, {txid: txToMerge.txid, balanceChange: txToMerge.balanceChange}));
    }

    let oldTxCount = await toClusterTransactionCountPromise;
    await this.db.writeBatchService.push(
      this.clusterTransactionCountTable.putOperation({clusterId: toCluster}, {transactionCount: oldTxCount+newTxCount})
    );
    let oldBalance = await toClusterBalancePromsie;
    let newBalance = oldBalance+fromClusterBalanceSum;
    if (oldBalance !== newBalance) {
      await this.db.writeBatchService.push(
        this.balanceToClusterTable.delOperation({balance: oldBalance, clusterId: toCluster})
      );
      await this.db.writeBatchService.push(
        this.balanceToClusterTable.putOperation({balance: newBalance, clusterId: toCluster}, {})
      );
      await this.db.writeBatchService.push(
        this.clusterBalanceTable.putOperation({clusterId: toCluster}, {balance: newBalance})
      );
    }
  }

  async mergeClusterTransactions(toCluster: number, ...fromClusters: number[]) {
    await this.mergeClusterTransactionsOps(toCluster, ...fromClusters);
    await this.db.writeBatchService.commit();
  }

}