import { ClusterTransaction } from '../models/cluster-transaction';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterBalanceTable } from '../tables/cluster-balance-table';
import { ClusterTransactionTable } from '../tables/cluster-transaction-table';
import { BinaryDB } from './binary-db';

export class ClusterTransactionService {
  
  clusterTransactionTable: ClusterTransactionTable;
  balanceToClusterTable: BalanceToClusterTable;
  clusterBalanceTable: ClusterBalanceTable;

  constructor(private db: BinaryDB) {
    this.clusterTransactionTable = new ClusterTransactionTable(db);
    this.balanceToClusterTable = new BalanceToClusterTable(db);
    this.clusterBalanceTable = new ClusterBalanceTable(db);
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
      let rs = this.clusterTransactionTable.createReadStream({
        gte: {clusterId: clusterId},
        lt: {clusterId: clusterId+1}
      });
      rs.on("data", function(data) {
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
    let transactionsFromPromises = [];
    let balancesFromPromises = [];
    fromClusters.forEach(fromCluster => {
      transactionsFromPromises.push(this.getClusterTransactions(fromCluster));
      balancesFromPromises.push(this.getClusterBalanceDefaultZero(fromCluster));
    });
    let fromClustersTransactions = await Promise.all(transactionsFromPromises);
    let fromClustersBalances = await Promise.all(balancesFromPromises);
    let txidToTransaction: Map<string, ClusterTransaction> = new Map();
    //let merged = [];
    let fromClustersBalanceSum = 0;
    for (const [index, fromCluster] of fromClusters.entries()) {
      let clusterTransactions: ClusterTransaction[] = fromClustersTransactions[index];
      let clusterBalance: number = fromClustersBalances[index];
      fromClustersBalanceSum += clusterBalance;
      for (const tx of clusterTransactions) {
        if (!txidToTransaction.has(tx.txid)) {
          txidToTransaction.set(tx.txid, tx);
        }
        await this.db.writeBatchService.push(
          this.clusterTransactionTable.delOperation({clusterId: fromCluster, height: tx.height, n: tx.n})
        );
      };
      await this.db.writeBatchService.push(
        this.clusterBalanceTable.delOperation({clusterId: fromCluster})
      );
      if (clusterTransactions.length > 0) {
        await this.db.writeBatchService.push(
          this.balanceToClusterTable.delOperation({balance: clusterBalance, clusterId: fromCluster})
        );
      }
    };
    for (const [txid, tx] of txidToTransaction) {
      await this.db.writeBatchService.push(this.clusterTransactionTable.putOperation({clusterId: toCluster, height: tx.height, n: tx.n}, {txid: tx.txid}));
    };
    let oldBalance = await this.getClusterBalanceDefaultZero(toCluster);
    let newBalance = oldBalance+fromClustersBalanceSum;
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