import { LevelUp } from 'levelup';

import { integer2LexString } from '../utils/utils';
import {
  db_address_cluster_prefix,
  db_cluster_address_count_prefix,
  db_cluster_address_prefix,
  db_next_cluster_id,
} from './db-constants';

console.log(db_cluster_address_prefix);
console.log(db_cluster_address_count_prefix);
console.log(db_address_cluster_prefix);
console.log(db_next_cluster_id);

export class ClusterAddressService {

  constructor(private db: LevelUp) {

  }  

  async mergeClusterAddresses(fromClusterId: string, toClusterId: string) {

    let addressCountPromise = new Promise<any>(function(resolve, reject) {
      this.db.get(db_cluster_address_count_prefix+toClusterId, (error, count) => {
        if (error) reject(error)
        else resolve(count);
      });
    });
  
    let addressesPromise = new Promise<any>((resolve, reject) => {
      let addresses: string[] = [];
      this.db.createValueStream({
        gte:db_cluster_address_prefix+fromClusterId+"/0",
        lt:db_cluster_address_prefix+fromClusterId+"/z"
      })
      .on('data', function (data) {
        addresses.push(data);
      })
      .on('error', function (err) {
        reject(err);
      })
      .on('close', function () {
        resolve(addresses);
      })
      .on('end', function () {
      });
    });
  
    let values = await Promise.all([addressCountPromise, addressesPromise]);
    let ops = [];
    let count = values[0];
    let addresses = values[1];
    addresses.forEach((address, index) => {
      let newIndex = count+index+1;
      ops.push({
        type:"put", 
        key: db_cluster_address_prefix+toClusterId+"/"+integer2LexString(newIndex), 
        value:address
      });
      ops.push({
        type:"del", 
        key: db_cluster_address_prefix+fromClusterId+"/"+integer2LexString(index)
      });
      ops.push({
        type:"put",
        key:db_address_cluster_prefix+address,
        value:toClusterId
      });
    });
    ops.push({
      type:"put", 
      key: db_cluster_address_count_prefix+toClusterId, 
      value: count+addresses.length
    });
    ops.push({
      type:"del", 
      key: db_cluster_address_count_prefix+fromClusterId
    });
  
    return this.db.batch(ops);
  }

  async addAddressesToCluster(addresses: string[], clusterId: string): Promise<void> {
    return new Promise<any>((resolve, reject) => {
      if (addresses.length === 0) resolve();
      this.db.get(db_cluster_address_count_prefix+clusterId, (error, count) => {
        let ops = [];
        ops.push({
          type:"put",
          key:db_cluster_address_count_prefix+clusterId,
          value:count+addresses.length
        });
        addresses.forEach((address, index) => {
          let newIndex = count+index+1;
          ops.push({
            type: "put",
            key: db_cluster_address_prefix+clusterId+"/"+integer2LexString(newIndex),
            value: address
          });
          ops.push({
            type: "put",
            key: db_address_cluster_prefix+address,
            value: clusterId
          });
        });
        this.db.batch(ops, (error) => {
          if (error) reject(error)
          else resolve();
        });
      });
    });
  }

  async createClusterWithAddresses(addresses: string[]): Promise<void> {
    let next_cluster_id: number = await this.db.get(db_next_cluster_id);
    let clusterId = next_cluster_id;
    next_cluster_id++;
    if (addresses.length === 0) return;
    let ops = [];
    ops.push({
      type:"put",
      key:db_next_cluster_id,
      value:next_cluster_id
    });
    ops.push({
      type:"put",
      key:db_cluster_address_count_prefix+clusterId,
      value:addresses.length
    });
    addresses.forEach((address, index) => {
      ops.push({
        type: "put",
        key: db_cluster_address_prefix+clusterId+"/"+integer2LexString(index),
        value: address
      });
      ops.push({
        type: "put",
        key: db_address_cluster_prefix+address,
        value: clusterId
      });
    });
    return this.db.batch(ops);
  }

}  