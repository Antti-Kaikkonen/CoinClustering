import { LevelUp } from 'levelup';

import { ClusterBalance } from '../models/cluster-balance';
import { integer2LexString } from '../utils/utils';
import {
  db_cluster_balance_count_prefix,
  db_cluster_balance_prefix,
  db_cluster_tx_balance_prefix,
  db_value_separator,
} from './db-constants';

console.log(db_cluster_balance_prefix);

console.log(db_cluster_tx_balance_prefix);

console.log(db_cluster_balance_count_prefix);

export class ClusterBalanceService {
  

  constructor(private db: LevelUp) {

  }  


  async getLast(clusterId: String): Promise<ClusterBalance> {
    
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
        let index = Number(key.substr((db_cluster_balance_prefix+clusterId+"/").length));
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

  async getBalance(clusterId) {
    return new Promise<number>((resolve, reject) => {
      this.getLast(clusterId).then(clusterBalanace => resolve(clusterBalanace.balance));
    });
  }

  async getClusterTransactions(clusterId: string): Promise<ClusterBalance[]> {
    return new Promise<ClusterBalance[]>((resolve, reject) => {
      let transactions = [];
      this.db.createReadStream({
        gte:db_cluster_balance_prefix+clusterId+"/0",
        lt:db_cluster_balance_prefix+clusterId+"/z"
      }).on("data", function(data) {
        let key: string = data.key;
        let index = Number(key.substr((db_cluster_balance_prefix+clusterId+"/").length));
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

  async mergeClusterTransactions(fromCluster: string, toCluster: string) {
    let values = await Promise.all([this.getClusterTransactions(fromCluster), this.getClusterTransactions(toCluster)]);
    let transactionsFrom = values[0].map((value, index, arr) => { 
      let delta = index === 0 ? value.balance : value.balance-arr[index-1].balance;
      return {txid: value.txid, delta: delta, height: value.height, n: value.n};
    });
  
    let transactionsTo = values[1].map((value, index, arr) => { 
      let delta = index === 0 ? value.balance : value.balance-arr[index-1].balance;
      return {txid: value.txid, delta: delta, height: value.height, n: value.n};
    });
  
  
    let merged = [];
    let i1 = 0;
    let i2 = 0;
  
    //Merge two sorted arrays into one sorted array
    while (i1 < transactionsFrom.length || i2 < transactionsTo.length) {
      if (
        i2 < transactionsTo.length &&
          (i1 === transactionsFrom.length || 
          transactionsTo[i2].height < transactionsFrom[i1].height || 
          (
            transactionsTo[i2].height === transactionsFrom[i1].height && 
            transactionsTo[i2].n < transactionsFrom[i1].n
          )
        )
      ) 
      {
        merged.push(transactionsTo[i2]);
        i2++;
      } else {
        merged.push(transactionsFrom[i1]);
        i1++;
      } 
    }
    let balances = [];
    balances[0] = merged[0].delta;
    for (let i = 1; i < merged.length; i++) {
      balances[i] = balances[i-1]+merged[i].delta;
    }
  
    let ops = [];
    merged.forEach((tx, index) => {
      ops.push({
        type: "put",
        key:db_cluster_balance_prefix+toCluster+"/"+integer2LexString(index),
        value: this.cluster_balance_value(tx.txid, balances[index], tx.height, tx.n)
      });
      ops.push({
        type: "put",
        key:db_cluster_tx_balance_prefix+toCluster+"/"+tx.txid,
        value: this.cluster_tx_balance_value(index, balances[index], tx.height, tx.n)
      });
    });
    transactionsFrom.forEach((tx, index) => {
      ops.push({
        type: "del",
        key:db_cluster_balance_prefix+fromCluster+"/"+integer2LexString(index)
      });
      ops.push({
        type: "del",
        key:db_cluster_tx_balance_prefix+fromCluster+"/"+tx.txid
      });
    });
    ops.push({
      type:"put",
      key:db_cluster_balance_count_prefix+toCluster,
      value:merged.length
    });
    ops.push({
      type:"del",
      key:db_cluster_balance_count_prefix+fromCluster
    });
  
    return this.db.batch(ops);
  
  }

  async getCurrentClusterBalance(clusterId: string) {
    return new Promise<{txid: string, balance: number, index: number}>((resolve, reject) => {
      let result;
      this.db.createReadStream({
        gte:db_cluster_balance_prefix+clusterId+"/0",
        lt:db_cluster_balance_prefix+clusterId+"/z",
        reverse: true,
        limit: 1
      })
      .on('data', function (data) {
        let key: string = data.key;
        let index = Number(key.substr((db_cluster_balance_prefix+clusterId+"/").length));
        let value: string = data.value;
        let valueComponents = value.split(db_value_separator);
        
        result = {txid:valueComponents[0], balance:valueComponents[1], index: index};
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


  async saveClusterBalanceChanges(txid: string, height: number, n: number, clusterIdToDelta: Map<string, number>) {
    let promises = [];
    let clusterIds = [];
    let deltas = [];
    clusterIdToDelta.forEach((delta: number, clusterId: string) => {
      clusterIds.push(clusterId);
      promises.push(this.getCurrentClusterBalance(clusterId));
      deltas.push(delta);
    });
    let oldBalances = Promise.all(promises);
    let ops = [];
    for (let i = 0; i < deltas.length; i++) {
      let clusterId = clusterIds[i];
      let index = oldBalances[i] === undefined ? 0 : oldBalances[i].index+1;
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