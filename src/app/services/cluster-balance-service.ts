import { AbstractBatch } from 'abstract-leveldown';
import { ClusterBalance } from '../models/cluster-balance';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterBalanceCountTable } from '../tables/cluster-balance-count-table';
import { ClusterBalanceTable } from '../tables/cluster-balance-table';
import { ClusterTxBalanceTable } from '../tables/cluster-tx-balance-table';
import { LastSavedTxNTable } from '../tables/last-saved-tx-n-table';
import { BinaryDB } from './binary-db';
import { db_value_separator } from './db-constants';

export class ClusterBalanceService {
  
  clusterBalanceTable: ClusterBalanceTable;
  clusterTxBalanceTable: ClusterTxBalanceTable;
  clusterBalanceCountTable: ClusterBalanceCountTable;
  lastSavedTxNTable: LastSavedTxNTable;
  balanceToClusterTable: BalanceToClusterTable;

  constructor(private db: BinaryDB) {
    this.clusterBalanceTable = new ClusterBalanceTable(db);
    this.clusterTxBalanceTable = new ClusterTxBalanceTable(db);
    this.clusterBalanceCountTable = new ClusterBalanceCountTable(db);
    this.lastSavedTxNTable = new LastSavedTxNTable(db);
    this.balanceToClusterTable = new BalanceToClusterTable(db);
  }  

  async getLast(clusterId: number): Promise<ClusterBalance> {
    
    return new Promise<ClusterBalance>((resolve, reject) => {
      let result: ClusterBalance;
      this.clusterBalanceTable.createReadStream({
        gte: {clusterId: clusterId, transactionIndex: 0},
        lt: {clusterId: clusterId, transactionIndex: Number.MAX_SAFE_INTEGER},
        reverse: true,
        limit: 1
      }).on('data', (data) => {
        result = new ClusterBalance(
          data.key.transactionIndex,
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

  async getClusterTransaction(clusterId: number, index: number): Promise<ClusterBalance> {
    let value = await this.clusterBalanceTable.get({clusterId: clusterId, transactionIndex: index});
    let cb = new ClusterBalance(index, value.txid, value.balance, value.height, value.n);
    return cb;
  }

  async getBalance(clusterId) {
    return new Promise<number>((resolve, reject) => {
      this.getLast(clusterId).then(clusterBalanace => resolve(clusterBalanace.balance));
    });
  }


  async firstTransactionAfter(clusterId: number, height: number, n: number, start: number, end: number): Promise<number> {
    while (start <= end) {
      let mid: number = Math.floor((start+end)/2);
      let tx: ClusterBalance = await this.getClusterTransaction(clusterId, mid);
      if (tx.height === height && tx.n === n) {
        return tx.id;
      } else if (tx.height < height || tx.height === height && tx.n < n) {
        start = mid+1;
      } else {
        end = mid-1;
      }
    }
    return start;
  }

  async getClusterTransactionsAfter(clusterId: number, height: number, n: number): Promise<ClusterBalance[]> {
    return new Promise<ClusterBalance[]>(async (resolve, reject) => {
      let res: ClusterBalance[] = [];
      let resolved = false;
      let rs = this.clusterBalanceTable.createReadStream({
        gte: {clusterId: clusterId, transactionIndex: 0},
        lt: {clusterId: clusterId, transactionIndex: Number.MAX_SAFE_INTEGER},
        reverse: true
      }).on('data', (data) => {
        let cb = new ClusterBalance(
          data.key.transactionIndex,
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

  async getClusterTransactions(clusterId: number): Promise<ClusterBalance[]> {
    return new Promise<ClusterBalance[]>((resolve, reject) => {
      let transactions = [];
      let rs = this.clusterBalanceTable.createReadStream({
        gte: {clusterId: clusterId, transactionIndex: 0},
        lt: {clusterId: clusterId, transactionIndex: Number.MAX_SAFE_INTEGER}
      });
      rs.on("data", function(data) {
        let cb = new ClusterBalance(
          data.key.transactionIndex,
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
    //console.log("MERGING CLUSTER TRANSACTIONS")
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    if (fromClusters.length === 0) return ops;
    let transactionsFromPromises = [];
    fromClusters.forEach(fromCluster => transactionsFromPromises.push(this.getClusterTransactions(fromCluster)));
    let values = await Promise.all(transactionsFromPromises);
    let merged = [];
    fromClusters.forEach((fromCluster: number, index: number) => {
      let clusterTransactions: ClusterBalance[] = values[index];
      let clusterTransactionsWithDelta = clusterTransactions.map((value: ClusterBalance, index: number, arr: ClusterBalance[]) => { 
        let delta = index === 0 ? value.balance : value.balance-arr[index-1].balance;
        return {id: value.id, txid: value.txid, delta: delta, height: value.height, n: value.n};
      });

      clusterTransactionsWithDelta.forEach((tx: any, index: number) => {
        merged.push(tx);
        ops.push(
          this.clusterBalanceTable.delOperation({clusterId: fromCluster, transactionIndex: tx.id})
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
        this.clusterBalanceTable.putOperation({clusterId: toCluster, transactionIndex: newIndex}, {txid: tx.txid, balance: balances[index], height: tx.height, n: tx.n})
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
  }

  async mergeClusterTransactions(toCluster: number, ...fromClusters: number[]) {
    return this.db.batchBinary(await this.mergeClusterTransactionsOps(toCluster, ...fromClusters));
  }

  async saveClusterBalanceChanges(txid: string, height: number, n: number, clusterIdToDelta: Map<number, number>) {
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
        this.clusterBalanceTable.putOperation({clusterId: clusterId, transactionIndex: index}, {txid: txid, balance: newBalance, height: height, n: n})
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
  }

}