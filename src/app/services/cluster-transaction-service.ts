import { ClusterTransaction } from '../models/cluster-balance';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterBalanceTable } from '../tables/cluster-balance-table';
import { ClusterTransactionCountTable } from '../tables/cluster-transaction-count-table';
import { ClusterTransactionTable } from '../tables/cluster-transaction-table';
import { LastSavedTxNTable } from '../tables/last-saved-tx-n-table';
import { BinaryDB } from './binary-db';
import { db_value_separator } from './db-constants';

export class ClusterTransactionService {
  
  clusterTransactionTable: ClusterTransactionTable;
  clusterTransactionCountTable: ClusterTransactionCountTable;
  lastSavedTxNTable: LastSavedTxNTable;
  balanceToClusterTable: BalanceToClusterTable;
  clusterBalanceTable: ClusterBalanceTable;

  constructor(private db: BinaryDB) {
    this.clusterTransactionTable = new ClusterTransactionTable(db);
    this.clusterTransactionCountTable = new ClusterTransactionCountTable(db);
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
          data.value.balanceDelta,
          data.key.height,
          data.key.n);
      }).on('error', function (err) {
        reject(err);
      })
      .on('close', function () {
        //resolve(result);
      })
      .on('finish', function () {
        resolve(result);
      });
    });  
  }

  async getTransactionCount(clusterId: number): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      this.clusterTransactionCountTable.get({clusterId: clusterId}).then((res: {balanceCount: number}) => {
        resolve(res.balanceCount);
      }, (error) => {
        resolve(0);
      });
    })
  }

  async getClusterBalance(clusterId: number): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      this.clusterBalanceTable.get({clusterId: clusterId}).then((res: {balance: number}) => {
        resolve(res.balance);
      },
      (error) => {
        resolve(0);
      });
    });
  }

  async getClusterTransaction(clusterId: number, height: number, n: number): Promise<ClusterTransaction> {
    let value = await this.clusterTransactionTable.get({clusterId: clusterId, height: height, n: n});
    let cb = new ClusterTransaction(value.txid, value.balanceDelta, height, n);
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
          data.value.balance,
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
        //resolve(res);
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
        //console.log("")
        let cb = new ClusterTransaction(
          data.value.txid,
          data.value.balanceDelta,
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

  private cluster_balance_value(txid: string, balance: number, height: number, n: number) {
    return txid+db_value_separator+balance+db_value_separator+height+db_value_separator+n;
  }
  
  private cluster_tx_balance_value(index: number, balance: number, height: number, n: number) {
    return index+db_value_separator+balance+db_value_separator+height+db_value_separator+n;
  }

  private sortAndRemoveDuplicates(arr: any[]) {
    arr.sort((a, b) => {
      if (a.height === b.height && a.n === b.n) {
        return 0;
      } else if (a.height < b.height || (a.height === b.height && a.n < b.n)) {
        return -1;
      } else {
        return 1;
      }
    });
    let i = 1;
    while (i < arr.length) {//remove duplicates
      if (arr[i].height === arr[i-1].height && arr[i].n === arr[i-1].n) {
        arr[i-1].delta = arr[i-1].delta+arr[i].delta;
        arr.splice(i, 1);
      } else {
        i++;
      }
    }
  }

  async mergeClusterTransactionsOps(toCluster: number, ...fromClusters: number[]): Promise<void> {
    //let ops: AbstractBatch<Buffer, Buffer>[] = [];
    if (fromClusters.length === 0) return;// ops;
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
    //fromClusters.forEach((fromCluster: number, index: number) => {
      let clusterTransactions: ClusterTransaction[] = fromClustersTransactions[index];
      let clusterBalance: number = fromClustersBalances[index];
      fromClustersBalanceSum += clusterBalance;
      for (const tx of clusterTransactions) {
      //clusterTransactions.forEach((tx: ClusterTransaction, index: number) => {
        let existingTransaction = txidToTransaction.get(tx.txid);
        if (existingTransaction !== undefined) {
          existingTransaction.balanceDelta += tx.balanceDelta;
        } else {
          txidToTransaction.set(tx.txid, tx);
        }
        await this.db.writeBatchService.push(
          this.clusterTransactionTable.delOperation({clusterId: fromCluster, height: tx.height, n: tx.n})
        );
      };
      await this.db.writeBatchService.push(
        this.clusterTransactionCountTable.delOperation({clusterId: fromCluster})
      );
      await this.db.writeBatchService.push(
        this.clusterBalanceTable.delOperation({clusterId: fromCluster})
      );
      if (clusterTransactions.length > 0) {
        await this.db.writeBatchService.push(
          this.balanceToClusterTable.delOperation({balance: clusterBalance, clusterId: fromCluster})
        );
      }
    };
    //TODO: if toCluster.transactions.legth < sum(fromClusters.tx.length) then get all toCluster transactions instead.
    let newTransactions = 0;
    let allTransactionsMerged = new Promise((resolve, reject) => {
      if (txidToTransaction.size === 0) resolve();
      let counter = 0;
      txidToTransaction.forEach((txToMerge: ClusterTransaction, txid: string) => {
        counter++;
        this.getClusterTransaction(toCluster, txToMerge.height, txToMerge.n).then((tx: ClusterTransaction) => {
          txToMerge.balanceDelta += tx.balanceDelta;
          counter--;
          if (counter === 0) resolve();
        },
        (error) => {
          newTransactions++;
          counter--;
          if (counter === 0) resolve();
        }
      );
      });
    });
    await allTransactionsMerged;
    //txidToTransaction.forEach((tx: ClusterTransaction, txid: string) => {
    for (const [txid, tx] of txidToTransaction) {
      await this.db.writeBatchService.push(this.clusterTransactionTable.putOperation({clusterId: toCluster, height: tx.height, n: tx.n}, {txid: tx.txid, balanceDelta: tx.balanceDelta}));
    };
    let oldBalanceCount = await this.getTransactionCount(toCluster);
    let oldBalance = await this.getClusterBalance(toCluster);
    let newBalance = oldBalance+fromClustersBalanceSum;
    await this.db.writeBatchService.push(
      this.clusterTransactionCountTable.putOperation({clusterId: toCluster}, {balanceCount: oldBalanceCount+newTransactions})
    );
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
    //return ops;
  }

  async mergeClusterTransactions(toCluster: number, ...fromClusters: number[]) {
    await this.mergeClusterTransactionsOps(toCluster, ...fromClusters);
    await this.db.writeBatchService.commit();
    //return this.db.batchBinary(await this.mergeClusterTransactionsOps(toCluster, ...fromClusters));
  }

}