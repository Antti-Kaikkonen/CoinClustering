import { AbstractBatch } from 'abstract-leveldown';
import { ClusterTransaction } from '../models/cluster-balance';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterBalanceCountTable } from '../tables/cluster-balance-count-table';
import { ClusterTransactionTable } from '../tables/cluster-transaction-table';
import { ClusterTxBalanceTable } from '../tables/cluster-tx-balance-table';
import { LastSavedTxNTable } from '../tables/last-saved-tx-n-table';
import { BinaryDB } from './binary-db';
import { db_value_separator } from './db-constants';

export class ClusterBalanceService {
  
  clusterTransactionTable: ClusterTransactionTable;
  clusterTxBalanceTable: ClusterTxBalanceTable;
  clusterBalanceCountTable: ClusterBalanceCountTable;
  lastSavedTxNTable: LastSavedTxNTable;
  balanceToClusterTable: BalanceToClusterTable;

  constructor(private db: BinaryDB) {
    this.clusterTransactionTable = new ClusterTransactionTable(db);
    this.clusterTxBalanceTable = new ClusterTxBalanceTable(db);
    this.clusterBalanceCountTable = new ClusterBalanceCountTable(db);
    this.lastSavedTxNTable = new LastSavedTxNTable(db);
    this.balanceToClusterTable = new BalanceToClusterTable(db);
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
          data.value.balance,
          data.value.height,
          data.value.n);
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

  /*async getClusterTransaction(clusterId: number, index: number): Promise<ClusterTransaction> {
    let value = await this.clusterTransactionTable.get({clusterId: clusterId, transactionIndex: index});
    let cb = new ClusterTransaction(index, value.txid, value.balanceDeltaSats, value.height, value.n);
    return cb;
  }*/

  async getClusterTransaction(clusterId: number, height: number, n: number): Promise<ClusterTransaction> {
    let value = await this.clusterTransactionTable.get({clusterId: clusterId, height: height, n: n});
    let cb = new ClusterTransaction(value.txid, value.balanceDeltaSats, height, n);
    return cb;
  }

  /*async getBalance(clusterId) {
    return new Promise<number>((resolve, reject) => {
      this.getLast(clusterId).then(clusterBalanace => resolve(clusterBalanace.balance));
    });
  }*/


  /*async firstTransactionAfter(clusterId: number, height: number, n: number, start: number, end: number): Promise<number> {
    while (start <= end) {
      let mid: number = Math.floor((start+end)/2);
      let tx: ClusterTransaction = await this.getClusterTransaction(clusterId, mid);
      if (tx.height === height && tx.n === n) {
        return tx.id;
      } else if (tx.height < height || tx.height === height && tx.n < n) {
        start = mid+1;
      } else {
        end = mid-1;
      }
    }
    return start;
  }*/

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
        let cb = new ClusterTransaction(
          data.value.txid,
          data.value.balance,
          data.value.height,
          data.value.n
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

  async mergeClusterTransactionsOps(toCluster: number, ...fromClusters: number[]): Promise<AbstractBatch<Buffer, Buffer>[]> {
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    if (fromClusters.length === 0) return ops;
    let transactionsFromPromises = [];
    fromClusters.forEach(fromCluster => transactionsFromPromises.push(this.getClusterTransactions(fromCluster)));
    let values = await Promise.all(transactionsFromPromises);
    let txidToTransaction: Map<string, ClusterTransaction> = new Map();
    //let merged = [];
    fromClusters.forEach((fromCluster: number, index: number) => {
      let clusterTransactions: ClusterTransaction[] = values[index];
      clusterTransactions.forEach((tx: ClusterTransaction, index: number) => {
        let existingTransaction = txidToTransaction.get(tx.txid);
        if (existingTransaction !== undefined) {
          existingTransaction.balanceDeltaSat += tx.balanceDeltaSat;
        }
        ops.push(
          this.clusterTransactionTable.delOperation({clusterId: fromCluster, height: tx.height, n: tx.n})
        );
        ops.push(
          this.clusterTxBalanceTable.delOperation({clusterId: fromCluster, txid:tx.txid})
        );
      });
      ops.push(
        this.clusterBalanceCountTable.delOperation({clusterId: fromCluster})
      );
    });

    //TODO: if toCluster.transactions.legth < sum(fromClusters.tx.length) then get all toCluster transactions instead.
    let newTransactions = 0;
    let allTransactionsMerged = new Promise((resolve, reject) => {
      let counter = 0;
      txidToTransaction.forEach((txToMerge: ClusterTransaction, txid: string) => {
        counter++;
        this.getClusterTransaction(toCluster, txToMerge.height, txToMerge.n).then((tx: ClusterTransaction) => {
          txToMerge.balanceDeltaSat += tx.balanceDeltaSat;
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
    txidToTransaction.forEach((tx: ClusterTransaction, txid: string) => {
      ops.push(this.clusterTransactionTable.putOperation({clusterId: toCluster, height: tx.height, n: tx.n}, {txid: tx.txid, balanceDeltaSats: tx.balanceDeltaSat}));
    });
    let oldBalanceCount = await this.clusterBalanceCountTable.get({clusterId: toCluster});
    this.clusterBalanceCountTable.putOperation({clusterId: toCluster}, {balanceCount: oldBalanceCount.balanceCount+newTransactions});
    return ops;
  }

  /*async mergeClusterTransactionsOps_OLD(toCluster: number, ...fromClusters: number[]): Promise<AbstractBatch<Buffer, Buffer>[]> {
    //console.log("MERGING CLUSTER TRANSACTIONS")
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    if (fromClusters.length === 0) return ops;
    let transactionsFromPromises = [];
    fromClusters.forEach(fromCluster => transactionsFromPromises.push(this.getClusterTransactions(fromCluster)));
    let values = await Promise.all(transactionsFromPromises);
    let merged = [];
    fromClusters.forEach((fromCluster: number, index: number) => {
      let clusterTransactions: ClusterTransaction[] = values[index];
      let clusterTransactionsWithDelta = clusterTransactions.map((value: ClusterTransaction, index: number, arr: ClusterTransaction[]) => { 
        let delta = index === 0 ? value.balance : value.balance-arr[index-1].balance;
        return {id: value.id, txid: value.txid, delta: delta, height: value.height, n: value.n};
      });

      clusterTransactionsWithDelta.forEach((tx: any, index: number) => {
        merged.push(tx);
        ops.push(
          this.clusterTransactionTable.delOperation({clusterId: fromCluster, transactionIndex: tx.id})
        );
        ops.push(
          this.clusterTxBalanceTable.delOperation({clusterId: fromCluster, txid:tx.txid})
        );
      });
      ops.push(
        this.clusterBalanceCountTable.delOperation({clusterId: fromCluster})
      );
      if (clusterTransactions.length > 0) {
        let balanceToDelete = clusterTransactions[clusterTransactions.length-1].balance;
        ops.push(this.balanceToClusterTable.delOperation({balance: balanceToDelete, clusterId:fromCluster}));
      }

    });
    this.sortAndRemoveDuplicates(merged);
    if (merged.length === 0) return ops;
    let transactionsTo = await this.getClusterTransactionsAfter(toCluster, merged[0].height, merged[0].n);//[0] = oldest.
    let lastBalanceBeforeMerge: number;
    let oldBalance: number;
    let skipped: number;
    if (transactionsTo.length === 0) {
      let asd = await this.getLast(toCluster);
      lastBalanceBeforeMerge = oldBalance = asd.balance;
      skipped = asd.id+1;
    } else {
      oldBalance = transactionsTo[transactionsTo.length-1].balance;
      if (transactionsTo[0].id === 0) {
        lastBalanceBeforeMerge = 0;
        skipped = 0;
      } else {
        let asd = await this.getClusterTransaction(toCluster, transactionsTo[0].id-1);
        lastBalanceBeforeMerge = asd.balance;
        skipped = asd.id+1;
      }
    }

    let transactionsToWithDelta = transactionsTo.map((value, index, arr) => { 
      let delta = index === 0 ? value.balance-lastBalanceBeforeMerge : arr[index].balance-arr[index-1].balance;
      return {id: value.id, txid: value.txid, delta: delta, height: value.height, n: value.n};
    });

    transactionsToWithDelta.forEach(e => merged.push(e));
    this.sortAndRemoveDuplicates(merged);
    let balances = [];
    balances[0] = lastBalanceBeforeMerge+merged[0].delta;
    for (let i = 1; i < merged.length; i++) {
      balances[i] = balances[i-1]+merged[i].delta;
    }
  
    merged.forEach((tx, index: number) => {
      let newIndex = index+skipped;
      ops.push(
        this.clusterTransactionTable.putOperation({clusterId: toCluster, transactionIndex: newIndex}, {txid: tx.txid, balanceDeltaSats: balances[index], height: tx.height, n: tx.n})
      );
      ops.push(
        this.clusterTxBalanceTable.putOperation({clusterId:toCluster, txid: tx.txid}, {transactionIndex: newIndex, balance: balances[index], height: tx.height, n: tx.n})
      );
    });
    ops.push(
      this.clusterBalanceCountTable.putOperation({clusterId: toCluster}, {balanceCount: merged.length+skipped})
    );

    if (balances.length > 0) {
      let newBalance = balances[balances.length-1];
      if (lastBalanceBeforeMerge !== newBalance) {
        ops.push(this.balanceToClusterTable.delOperation({balance: oldBalance, clusterId: toCluster}));
        ops.push(this.balanceToClusterTable.putOperation({balance: newBalance, clusterId: toCluster}, {}));
      }
    }
    return ops;
  }*/

  async mergeClusterTransactions(toCluster: number, ...fromClusters: number[]) {
    return this.db.batchBinary(await this.mergeClusterTransactionsOps(toCluster, ...fromClusters));
  }

  /*async saveClusterBalanceChanges(txid: string, height: number, n: number, clusterIdToDelta: Map<number, number>) {
    //console.log("saveClusterBalanceChanges", clusterIdToDelta);
    let promises:Promise<ClusterBalance>[] = [];
    let clusterIds: number[] = [];
    let deltas = [];
    clusterIdToDelta.forEach((delta: number, clusterId: number) => {
      clusterIds.push(clusterId);
      promises.push(this.getLast(clusterId));
      deltas.push(delta);
    });
    let oldBalances = await Promise.all(promises);
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    for (let i = 0; i < deltas.length; i++) {
      let clusterId = clusterIds[i];
      let index = oldBalances[i] === undefined ? 0 : oldBalances[i].id+1;
      let oldBalance = oldBalances[i] === undefined ? 0 : oldBalances[i].balance;
      let newBalance = oldBalance+deltas[i];

      ops.push(this.clusterBalanceCountTable.putOperation({clusterId: clusterId}, {balanceCount: index+1}));
      ops.push(
        this.clusterBalanceTable.putOperation({clusterId: clusterId, transactionIndex: index}, {txid: txid, balanceDeltaSats: newBalance, height: height, n: n})
      );
      ops.push(
        this.clusterTxBalanceTable.putOperation({clusterId: clusterId, txid:txid}, {transactionIndex: index, balance: newBalance, height: height, n: n})
      );

      ops.push(this.balanceToClusterTable.putOperation({balance: newBalance, clusterId:clusterId}, {}));
      if (index > 0 && oldBalance !== newBalance) {
        ops.push(this.balanceToClusterTable.delOperation({balance: oldBalance, clusterId:clusterId}));
      }
    }
    ops.push(
      this.lastSavedTxNTable.putOperation(undefined, {n: n})
    );
    return this.db.batchBinary(ops);
  }*/

}