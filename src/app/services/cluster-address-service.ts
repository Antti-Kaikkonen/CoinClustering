import { AbstractBatch } from 'abstract-leveldown';
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

  async mergeClusterAddressesOps(toClusterId: number, fromClusterIds: number[], nonClusterAddresses?: string[]): Promise<AbstractBatch[]> {
    let ops: AbstractBatch[] = [];
    if (fromClusterIds.length === 0 && (nonClusterAddresses === undefined || nonClusterAddresses.length === 0)) return ops;
    let promises: Promise<any>[] = [];
    promises.push(this.db.get(db_cluster_address_count_prefix+toClusterId));

    fromClusterIds.forEach(fromClusterId => {
      promises.push(this.getClusterAddresses(fromClusterId));
    });
  
    let values = await Promise.all(promises);
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
        //console.log(ops[ops.length-1]);
        nextIndex++;
      });
      ops.push({
        type:"del", 
        key: db_cluster_address_count_prefix+fromClusterId
      });
    });
    if (nonClusterAddresses !== undefined && nonClusterAddresses.length > 0) {
      ops.push(... await this.addAddressesToClusterOps(nonClusterAddresses, toClusterId, nextIndex));
    } else {
      ops.push({
        type:"put", 
        key: db_cluster_address_count_prefix+toClusterId, 
        value: nextIndex
      });
    }
    return ops;
  }

  async mergeClusterAddresses(toClusterId: number, ...fromClusterIds: number[]) {
    return this.db.batch(await this.mergeClusterAddressesOps(toClusterId, fromClusterIds));
  }

  async addAddressesToClusterOps(addresses: string[], clusterId: number, oldClusterAddressCount?: number) {
    let ops: AbstractBatch[] = [];
    if (oldClusterAddressCount === undefined) {
      oldClusterAddressCount = Number(await this.db.get(db_cluster_address_count_prefix+clusterId));
    }
    ops.push({
      type:"put",
      key:db_cluster_address_count_prefix+clusterId,
      value:Number(oldClusterAddressCount)+addresses.length
    });
    addresses.forEach((address, index) => {
      let newIndex: number = Number(oldClusterAddressCount)+index;
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
    return ops;
  }

  async addAddressesToCluster(addresses: string[], clusterId: number): Promise<void> {
    return this.db.batch(await this.addAddressesToClusterOps(addresses, clusterId));
  }

  async createAddressClustersOps(clusterAddresses: string[], clusterId: number): Promise<AbstractBatch[]> {
    let ops: AbstractBatch[] = [];
    if (clusterAddresses.length === 0) throw new Error("createAddressClustersOps called with 0 addresses");
    ops.push({
      type:"put",
      key:db_cluster_address_count_prefix+clusterId,
      value:clusterAddresses.length
    });
    clusterAddresses.forEach((address: string, index: number) => {
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
    return ops;
  }  

  async createMultipleAddressClusters(clusters: Array<string[]>, next_cluster_id?: number) {
    if (next_cluster_id === undefined) {
      try {
        next_cluster_id = Number(await this.db.get(db_next_cluster_id));
      } catch (error) {
        next_cluster_id = 0;
      }
    }
    let ops = [];
    for (let clusterAddresses of clusters) {
      let newOps = await this.createAddressClustersOps(clusterAddresses, next_cluster_id);
      newOps.forEach(op => ops.push(op));
      next_cluster_id++;
    }
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