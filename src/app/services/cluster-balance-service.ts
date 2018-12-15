import { AbstractBatch } from 'abstract-leveldown';
import { LevelUp } from 'levelup';
import { ClusterBalance } from '../models/cluster-balance';
import { integer2LexString, lexString2Integer } from '../utils/utils';
import { db_cluster_balance_count_prefix, db_cluster_balance_prefix, db_cluster_tx_balance_prefix, db_value_separator } from './db-constants';

export class ClusterBalanceService {
  
  constructor(private db: LevelUp) {
  }  

  async getLast(clusterId: number): Promise<ClusterBalance> {
    
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
    });
  }

  async getClusterTransaction(clusterId: number, index: number): Promise<ClusterBalance> {
    let value = await this.db.get(db_cluster_balance_prefix+clusterId+"/"+integer2LexString(index));
    let valueComponents = value.split(db_value_separator);
    let cb = new ClusterBalance(index, valueComponents[0], Number(valueComponents[1]), Number(valueComponents[2]), Number(valueComponents[3]));
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
      let rs = this.db.createReadStream({
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
      });
    });
  }  

  async getClusterTransactions(clusterId: number): Promise<ClusterBalance[]> {
    return new Promise<ClusterBalance[]>((resolve, reject) => {
      let transactions = [];
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

  async mergeClusterTransactions(toCluster: number, ...fromClusters: number[]) {
    if (fromClusters.length === 0) return;
    let transactionsFromPromises = [];
    fromClusters.forEach(fromCluster => transactionsFromPromises.push(this.getClusterTransactions(fromCluster)));
    let values = await Promise.all(transactionsFromPromises);
    
    let merged = [];
    let ops: AbstractBatch[] = [];
    fromClusters.forEach((fromCluster: number, index: number) => {
      let transactionsFrom = values[index].map((value: ClusterBalance, index: number, arr: ClusterBalance[]) => { 
        let delta = index === 0 ? value.balance : value.balance-arr[index-1].balance;
        return {id: value.id, txid: value.txid, delta: delta, height: value.height, n: value.n};
      });

      transactionsFrom.forEach((tx: any, index: number) => {
        merged.push(tx);
        ops.push({
          type: "del",
          key:db_cluster_balance_prefix+fromCluster+"/"+integer2LexString(tx.id)
        });
        ops.push({
          type: "del",
          key:db_cluster_tx_balance_prefix+fromCluster+"/"+tx.txid
        });
      });
      ops.push({
        type:"del",
        key:db_cluster_balance_count_prefix+fromCluster
      });
    });
    this.sortAndRemoveDuplicates(merged);

    let transactionsTo = await this.getClusterTransactionsAfter(toCluster, merged[0].height, merged[0].n);//[0] = oldest.

    let oldBalance: number;
    let skipped: number;
    if (transactionsTo.length === 0) {
      let asd = await this.getLast(toCluster);
      oldBalance = asd.balance;
      skipped = asd.id+1;
    } else {
      if (transactionsTo[0].id === 0) {
        oldBalance = 0;
        skipped = 0;
      } else {
        let asd = await this.getClusterTransaction(toCluster, transactionsTo[0].id-1);
        oldBalance = asd.balance;
        skipped = asd.id+1;
      }
    }

    let transactionsToWithDelta = transactionsTo.map((value, index, arr) => { 
      let delta = index === 0 ? value.balance-oldBalance : value.balance-arr[index-1].balance;
      return {id: value.id, txid: value.txid, delta: delta, height: value.height, n: value.n};
    });

    transactionsToWithDelta.forEach(e => merged.push(e));
    this.sortAndRemoveDuplicates(merged);

    let balances = [];
    balances[0] = oldBalance+merged[0].delta;
    for (let i = 1; i < merged.length; i++) {
      balances[i] = balances[i-1]+merged[i].delta;
    }
  
    merged.forEach((tx, index: number) => {
      let newIndex = index+skipped;
      ops.push({
        type: "put",
        key:db_cluster_balance_prefix+toCluster+"/"+integer2LexString(newIndex),
        value: this.cluster_balance_value(tx.txid, balances[index], tx.height, tx.n)
      });
      ops.push({
        type: "put",
        key:db_cluster_tx_balance_prefix+toCluster+"/"+tx.txid,
        value: this.cluster_tx_balance_value(newIndex, balances[index], tx.height, tx.n)
      });
    });
    ops.push({
      type:"put",
      key:db_cluster_balance_count_prefix+toCluster,
      value:merged.length+skipped
    });
    return this.db.batch(ops);
  }

  async saveClusterBalanceChanges(txid: string, height: number, n: number, clusterIdToDelta: Map<string, number>) {
    let promises:Promise<ClusterBalance>[] = [];
    let clusterIds = [];
    let deltas = [];
    clusterIdToDelta.forEach((delta: number, clusterId: string) => {
      clusterIds.push(clusterId);
      promises.push(this.getLast(Number(clusterId)));
      deltas.push(delta);
    });
    let oldBalances = await Promise.all(promises);
    let ops: AbstractBatch[] = [];
    for (let i = 0; i < deltas.length; i++) {
      let clusterId = clusterIds[i];
      let index = oldBalances[i] === undefined ? 0 : oldBalances[i].id+1;
      let oldBalance = oldBalances[i] === undefined ? 0 : oldBalances[i].balance;
      let newBalance = oldBalance+deltas[i];
      ops.push({
        type:"put",
        key:db_cluster_balance_prefix+clusterId+"/"+integer2LexString(index),
        value:this.cluster_balance_value(txid, newBalance, height, n)
      });
      ops.push({
        type:"put",
        key:db_cluster_tx_balance_prefix+clusterId+"/"+txid,
        value:this.cluster_tx_balance_value(index, newBalance, height, n)
      });
    }
    return this.db.batch(ops);
  }

}