import { LevelUp } from 'levelup';
import { integer2LexString } from '../utils/utils';
import { db_address_cluster_prefix, db_cluster_address_count_prefix, db_cluster_address_prefix, db_next_cluster_id } from './db-constants';



export class ClusterAddressService {

  constructor(private db: LevelUp) {

  }  

  async getAddressCluster(address: string): Promise<number> {
    let clusterStr = await this.db.get(db_address_cluster_prefix+address);
    return Number(clusterStr);
  }

  async getClusterAddresses(clusterId: number): Promise<string[]> {
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

  async mergeClusterAddresses(toClusterId: number, ...fromClusterIds: number[]) {
    if (fromClusterIds.length === 0) return;
    let promises: Promise<any>[] = [];
    let addressCountPromise = new Promise<any>((resolve, reject) => {
      this.db.get(db_cluster_address_count_prefix+toClusterId, (error, count) => {
        if (error) reject(error)
        else resolve(count);
      });
    });
    promises.push(addressCountPromise);
  
    fromClusterIds.forEach(fromClusterId => {
      promises.push(this.getClusterAddresses(fromClusterId));
    });
  
    let values = await Promise.all(promises);
    let ops = [];
    let nextIndex = Number(values[0]);

    fromClusterIds.forEach((fromClusterId: number, index: number) => {
      let addresses: string[] = values[1+index];
      addresses.forEach((address: string, index: number) => {
        ops.push({
          type:"put", 
          key: db_cluster_address_prefix+toClusterId+"/"+integer2LexString(nextIndex), 
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
        nextIndex++;
      });
      ops.push({
        type:"del", 
        key: db_cluster_address_count_prefix+fromClusterId
      });
    });
    ops.push({
      type:"put", 
      key: db_cluster_address_count_prefix+toClusterId, 
      value: nextIndex
    });
    return this.db.batch(ops);
  }

  async addAddressesToCluster(addresses: string[], clusterId: number): Promise<void> {
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