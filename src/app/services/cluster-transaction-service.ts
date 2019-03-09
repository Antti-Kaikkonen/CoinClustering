import { injectable } from 'inversify';
import { Writable } from 'stream';
import { ClusterTransaction } from '../models/cluster-transaction';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterBalanceTable } from '../tables/cluster-balance-table';
import { ClusterTransactionTable } from '../tables/cluster-transaction-table';
import { BinaryDB } from './binary-db';


@injectable()
export class ClusterTransactionService {

  constructor(private db: BinaryDB,
    private clusterTransactionTable: ClusterTransactionTable,
    private balanceToClusterTable: BalanceToClusterTable,
    private clusterBalanceTable: ClusterBalanceTable
  ) {
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

  async getClusterTransaction(clusterId: number, height: number, n: number): Promise<ClusterTransaction> {
    let value = await this.clusterTransactionTable.get({clusterId: clusterId, height: height, n: n});
    let cb = new ClusterTransaction(value.txid, height, n);
    return cb;
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
    let fromClusterBalanceSum: number = 0;
    let txIdsMerged: Set<string> = new Set();
    let allClustersMerged = new Promise((resolve, reject) => {
      let clustersToMerge: number = fromClusters.length;
      if (clustersToMerge === 0) resolve();
      fromClusters.forEach(fromCluster => {
        let clusterBalancePromise = this.getClusterBalanceDefaultZero(fromCluster);
        let clusterTransactionCount = 0;

        let txMerger = new Writable({
          objectMode: true,
          write: async (data: {key: {height: number, n: number}, value: {txid: string}}, encoding, callback) => {
            clusterTransactionCount++;
            let tx: ClusterTransaction = new ClusterTransaction(
              data.value.txid,
              data.key.height,
              data.key.n
            );
            if (!txIdsMerged.has(tx.txid)) {
              txIdsMerged.add(tx.txid);
              await this.db.writeBatchService.push(this.clusterTransactionTable.putOperation({clusterId: toCluster, height: tx.height, n: tx.n}, {txid: tx.txid}));
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