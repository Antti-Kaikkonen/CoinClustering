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
      .on('end', function () {
        resolve(result);
      });
    });  
/*
    return new Promise<ClusterBalance>((resolve, reject) => {
      let result: ClusterBalance;
      this.db.createReadStream({
        gte:db_cluster_balance_prefix+clusterId+"/0",
        lt:db_cluster_balance_prefix+clusterId+"/z",
        reverse: true,
        limit: 1
      })
      .on('data', function (data) {
        let key: string = data.key;
        let index = lexString2Integer(key.substr((db_cluster_balance_prefix+clusterId+"/").length));
        let value: string = data.value;
        let valueComponents = value.split(db_value_separator);
        
        result = new ClusterBalance(index, valueComponents[0], Number(valueComponents[1]), Number(valueComponents[2]), Number(valueComponents[3]));
      })
      .on('error', function (err) {
        reject(err);
      })
      .on('close', function () {
        resolve(result);
      })
      .on('end', function () {
      });
    });*/
  }

  async getClusterTransaction(clusterId: number, index: number): Promise<ClusterBalance> {
    let value = await this.clusterBalanceTable.get({clusterId: clusterId, transactionIndex: index});
    //let value = await this.db.get(db_cluster_balance_prefix+clusterId+"/"+integer2LexString(index));
    //let valueComponents = value.split(db_value_separator);
    let cb = new ClusterBalance(index, value.txid, value.balance, value.height, value.n);
    //let cb = new ClusterBalance(index, valueComponents[0], Number(valueComponents[1]), Number(valueComponents[2]), Number(valueComponents[3]));
    return cb;
  }

  async getBalance(clusterId) {
    return new Promise<number>((resolve, reject) => {
      this.getLast(clusterId).then(clusterBalanace => resolve(clusterBalanace.balance));
    });
  }


  async getClusterTransactionsAfter(clusterId: number, height: number, n: number): Promise<ClusterBalance[]> {
    

    return new Promise<ClusterBalance[]>(async (resolve, reject) => {
      let res: ClusterBalance[] = [];

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
          res.unshift(cb);
        } else {
          resolve(res);
          rs['destroy']('no error');
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
        resolve(res);
      });

      /*let rs = this.db.createReadStream({
        gte:db_cluster_balance_prefix+clusterId+"/0",
        lt:db_cluster_balance_prefix+clusterId+"/z",
        reverse: true
      });
      rs.on('data', function (data) {
        let key: string = data.key;
        let index = lexString2Integer(key.substr((db_cluster_balance_prefix+clusterId+"/").length));
        let value: string = data.value;
        let valueComponents = value.split(db_value_separator);
        
        let cb = new ClusterBalance(index, valueComponents[0], Number(valueComponents[1]), Number(valueComponents[2]), Number(valueComponents[3]));
        if (cb.height > height || cb.height === height && cb.n >= n) {
          res.unshift(cb);
        } else {
          resolve(res);
          rs['destroy']('no error');
        }  
      })
      .on('error', function (err) {
        if (err !== "no error") {
          console.log(err);
          reject(err);
        }
      })
      .on('close', function () {
        resolve(res);
      })
      .on('end', function () {
      });*/
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
      .on('close', function () {
        //resolve(transactions);
      })
      .on('end', function () {
        resolve(transactions);
      });
      /*
      let rs = this.db.createReadStream({
        gte:db_cluster_balance_prefix+clusterId+"/0",
        lt:db_cluster_balance_prefix+clusterId+"/z"
      });
      rs.on("data", function(data) {
        let key: string = data.key;
        let index = lexString2Integer(key.substr((db_cluster_balance_prefix+clusterId+"/").length));
        let value: string = data.value;
        let valueComponents = value.split(db_value_separator);
        transactions.push(
          new ClusterBalance( index, valueComponents[0], Number(valueComponents[1]), Number(valueComponents[2]), Number(valueComponents[3]) )
        );
      })
      .on('error', function (err) {
        reject(err);
      })
      .on('close', function () {
        resolve(transactions);
      })
      .on('end', function () {
      });*/
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
        /*ops.push({
          type: "del",
          key:db_cluster_balance_prefix+fromCluster+"/"+integer2LexString(tx.id)
        });*/
        ops.push(
          this.clusterTxBalanceTable.delOperation({clusterId: fromCluster, txid:tx.txid})
        );
        /*ops.push({
          type: "del",
          key:db_cluster_tx_balance_prefix+fromCluster+"/"+tx.txid
        });*/
      });
      ops.push(
        this.clusterBalanceCountTable.delOperation({clusterId: fromCluster})
      );
      if (clusterTransactions.length > 0) {
        let balanceToDelete = clusterTransactions[clusterTransactions.length-1].balance;
        ops.push(this.balanceToClusterTable.delOperation({balance: balanceToDelete, clusterId:fromCluster}));
      }
      /*let buf = Buffer.allocUnsafe(4);
      buf.writeUInt32BE(fromCluster, 0);
      ops.push({
        type:"del",
        key:Buffer.concat([db_cluster_balance_count_prefix, buf])
      });*/
    });
    this.sortAndRemoveDuplicates(merged);
    if (merged.length === 0) return ops;
    let transactionsTo = await this.getClusterTransactionsAfter(toCluster, merged[0].height, merged[0].n);//[0] = oldest.
    //transactionsTo[0].height <= transactionsTo[1].height
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
      let delta = index === 0 ? value.balance-lastBalanceBeforeMerge : value.balance-arr[index-1].balance;
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
      /*ops.push({
        type: "put",
        key:db_cluster_balance_prefix+toCluster+"/"+integer2LexString(newIndex),
        value: this.cluster_balance_value(tx.txid, balances[index], tx.height, tx.n)
      });*/
      ops.push(
        this.clusterTxBalanceTable.putOperation({clusterId:toCluster, txid: tx.txid}, {transactionIndex: newIndex, balance: balances[index], height: tx.height, n: tx.n})
      );
      /*ops.push({
        type: "put",
        key:db_cluster_tx_balance_prefix+toCluster+"/"+tx.txid,
        value: this.cluster_tx_balance_value(newIndex, balances[index], tx.height, tx.n)
      });*/
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
    /*ops.push({
      type:"put",
      key:db_cluster_balance_count_prefix+toCluster,
      value:merged.length+skipped
    });*/
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
      //console.log("\t",clusterId, oldBalance, newBalance, index);
      ops.push(
        this.clusterBalanceTable.putOperation({clusterId: clusterId, transactionIndex: index}, {txid: txid, balance: newBalance, height: height, n: n})
      );
      /*ops.push({
        type:"put",
        key:db_cluster_balance_prefix+clusterId+"/"+integer2LexString(index),
        value:this.cluster_balance_value(txid, newBalance, height, n)
      });*/
      ops.push(
        this.clusterTxBalanceTable.putOperation({clusterId: clusterId, txid:txid}, {transactionIndex: index, balance: newBalance, height: height, n: n})
      );
      /*ops.push({
        type:"put",
        key:db_cluster_tx_balance_prefix+clusterId+"/"+txid,
        value:this.cluster_tx_balance_value(index, newBalance, height, n)
      });*/

      ops.push(this.balanceToClusterTable.putOperation({balance: newBalance, clusterId:clusterId}, {}));
      if (index > 0 && oldBalance !== newBalance) {
        ops.push(this.balanceToClusterTable.delOperation({balance: oldBalance, clusterId:clusterId}));
      }
    }
    ops.push(
      this.lastSavedTxNTable.putOperation(undefined, {n: n})
    );
    /*ops.push({
      type: "put",
      key: db_last_saved_tx_n, 
      value: n
    });*/
    return this.db.batchBinary(ops);
  }

}