import { ClusterTransaction } from '../models/cluster-balance';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterBalanceTable } from '../tables/cluster-balance-table';
import { ClusterTransactionTable } from '../tables/cluster-transaction-table';
import { LastSavedTxNTable } from '../tables/last-saved-tx-n-table';
import { BinaryDB } from './binary-db';

export class ClusterTransactionService {
  
  clusterTransactionTable: ClusterTransactionTable;
  lastSavedTxNTable: LastSavedTxNTable;
  balanceToClusterTable: BalanceToClusterTable;
  clusterBalanceTable: ClusterBalanceTable;

  constructor(private db: BinaryDB) {
    this.clusterTransactionTable = new ClusterTransactionTable(db);
    this.lastSavedTxNTable = new LastSavedTxNTable(db);
    this.balanceToClusterTable = new BalanceToClusterTable(db);
    this.clusterBalanceTable = new ClusterBalanceTable(db);
  }  

  async getLast(clusterId: number): Promise<ClusterTransaction> {
    return new Promise<ClusterTransaction>((resolve, reject) => {
      let result: ClusterTransaction;
      this.clusterTransactionTable.createReadStream({
        gte: {clusterId: clusterId},
        lt: {clusterId: clusterId+1},
        reverse: true,
        limit: 1
      }).on('data', (data) => {
        result = new ClusterTransaction(
          data.value.txid,
          data.key.height,
          data.key.n);
      }).on('error', function (err) {
        reject(err);
      })
      .on('close', function () {
      })
      .on('finish', function () {
        resolve(result);
      });
    });  
  }


  async getClusterBalanceWithUndefined(clusterId: number): Promise<number> {
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

  async getClusterBalance(clusterId: number): Promise<number> {
    let res = await this.getClusterBalanceWithUndefined(clusterId);
    if (res === undefined) res = 0;
    return res;
  }

  async getClusterTransaction(clusterId: number, height: number, n: number): Promise<ClusterTransaction> {
    let value = await this.clusterTransactionTable.get({clusterId: clusterId, height: height, n: n});
    let cb = new ClusterTransaction(value.txid/*, value.balanceDelta*/, height, n);
    return cb;
  }

  async getClusterTransactionsAfter(clusterId: number, height: number, n: number): Promise<ClusterTransaction[]> {
    return new Promise<ClusterTransaction[]>(async (resolve, reject) => {
      let res: ClusterTransaction[] = [];
      let resolved = false;
      let rs = this.clusterTransactionTable.createReadStream({
        gte: {clusterId: clusterId},
        lt: {clusterId: clusterId+1},
        reverse: true
      }).on('data', (data) => {
        let cb = new ClusterTransaction(
          data.value.txid,
          data.value.height,
          data.value.n
        );
        if (cb.height > height || cb.height === height && cb.n >= n) {
          res.push(cb);
        } else {
          if (!resolved) {
            resolved = true;
            res = res.reverse();
            resolve(res);
            rs['destroy']('no error');
          }
        }    
      }).on('error', function (err) {
        if (err !== "no error") {
          console.log(err);
          reject(err);
        }
      })
      .on('close', function () {
      })
      .on('end', function () {
        if (!resolved) {
          resolved = true;
          res = res.reverse();
          resolve(res);
        }
      });
    });
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
      balancesFromPromises.push(this.getClusterBalance(fromCluster));
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
    let oldBalance = await this.getClusterBalance(toCluster);
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