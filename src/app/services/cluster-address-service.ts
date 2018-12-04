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

  async getAddressCluster(address: string): Promise<string> {
    return this.db.get(db_address_cluster_prefix+address);
  }

  async getClusterAddresses(clusterId: string): Promise<string[]> {
    return new Promise<any>((resolve, reject) => {
      let addresses: string[] = [];
      this.db.createValueStream({
        gte:db_cluster_address_prefix+clusterId+"/0",
        lt:db_cluster_address_prefix+clusterId+"/z"
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
  }

  async mergeClusterAddresses(fromClusterId: string, toClusterId: string) {

    let addressCountPromise = new Promise<any>((resolve, reject) => {
      this.db.get(db_cluster_address_count_prefix+toClusterId, (error, count) => {
        if (error) reject(error)
        else resolve(count);
      });
    });
  
    let addressesPromise = this.getClusterAddresses(fromClusterId);
  
    let values = await Promise.all([addressCountPromise, addressesPromise]);
    let ops = [];
    let count = Number(values[0]);
    let addresses: string[] = values[1];
    addresses.forEach((address: string, index: number) => {
      let newIndex = count+index;
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
      if (addresses.length === 0) {
        resolve();
        return;
      }  
      this.db.get(db_cluster_address_count_prefix+clusterId, (error, count) => {
        let ops = [];
        ops.push({
          type:"put",
          key:db_cluster_address_count_prefix+clusterId,
          value:Number(count)+addresses.length
        });
        addresses.forEach((address, index) => {
          let newIndex = Number(count)+index+1;
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

  async createMultipleAddressClusters(clusters: Array<string[]>) {
    let next_cluster_id: number;
    try {
      next_cluster_id = Number(await this.db.get(db_next_cluster_id));
    } catch (error) {
      next_cluster_id = 0;
    }
    let ops = [];
    clusters.forEach((clusterAddresses: string[]) => {
      if (clusterAddresses.length === 0) return;
      ops.push({
        type:"put",
        key:db_cluster_address_count_prefix+next_cluster_id,
        value:clusterAddresses.length
      });
      clusterAddresses.forEach((address: string, index: number) => {
        ops.push({
          type: "put",
          key: db_cluster_address_prefix+next_cluster_id+"/"+integer2LexString(index),
          value: address
        });
        ops.push({
          type: "put",
          key: db_address_cluster_prefix+address,
          value: next_cluster_id
        });
      });
      next_cluster_id++;
    });
    ops.push({
      type:"put",
      key:db_next_cluster_id,
      value:next_cluster_id
    });
    return this.db.batch(ops);
  }

  async createClusterWithAddresses(addresses: string[]): Promise<void> {
    return this.createMultipleAddressClusters([addresses]);
  }

}  