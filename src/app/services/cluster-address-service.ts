import { AbstractBatch } from 'abstract-leveldown';
import { AddressClusterTable } from '../tables/address-cluster-table';
import { ClusterAddressCountTable } from '../tables/cluster-address-count-table';
import { ClusterAddressTable } from '../tables/cluster-address-table';
import { NextClusterIdTable } from '../tables/next-cluster-id-table';
import { AddressEncodingService } from './address-encoding-service';
//import { integer2LexString } from '../utils/utils';
import { BinaryDB } from './binary-db';



export class ClusterAddressService {

  clusterAddressTable: ClusterAddressTable;
  clusterAddressCountTable: ClusterAddressCountTable;
  nextClusterIdTable: NextClusterIdTable;
  addressClusterTable: AddressClusterTable;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService) {
    this.clusterAddressTable = new ClusterAddressTable(db, addressEncodingService);
    this.clusterAddressCountTable = new ClusterAddressCountTable(db);
    this.nextClusterIdTable = new NextClusterIdTable(db);
    this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
  }  

  async getAddressCluster(address: string): Promise<number> {
    return (await this.addressClusterTable.get({address: address})).clusterId;
  }


  async getClusterAddresses(clusterId: number): Promise<string[]> {
    return new Promise<string[]>((resolve, reject) => {
      let addresses: string[] = [];
      this.clusterAddressTable.createReadStream({
        gte: {clusterId: clusterId, addressIndex: 0},
        lt: {clusterId: clusterId, addressIndex: Number.MAX_SAFE_INTEGER}
      }).on('data', function (data) {
        addresses.push(data.value.address);
      })
      .on('error', function (err) {
        reject(err);
      })
      .on('close', function () {
        //resolve(addresses);
      })
      .on('end', function () {
        resolve(addresses);
      });
    });
  }

  async mergeClusterAddressesOps(toClusterId: number, fromClusterIds: number[], nonClusterAddresses?: string[]): Promise<AbstractBatch<Buffer, Buffer>[]> {
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    if (fromClusterIds.length === 0 && (nonClusterAddresses === undefined || nonClusterAddresses.length === 0)) return ops;
    let promises: Promise<any>[] = [];
    promises.push(this.clusterAddressCountTable.get({clusterId: toClusterId}));

    fromClusterIds.forEach(fromClusterId => {
      promises.push(this.getClusterAddresses(fromClusterId));
    });
  
    let values = await Promise.all(promises);
    let nextIndex = values[0].addressCount;

    fromClusterIds.forEach((fromClusterId: number, index: number) => {
      let addresses: string[] = values[1+index];
      addresses.forEach((address: string, index: number) => {
        ops.push(
          this.clusterAddressTable.putOperation({clusterId: toClusterId, addressIndex: nextIndex}, {address: address})
        );
        ops.push(
          this.clusterAddressTable.delOperation({clusterId: fromClusterId, addressIndex: index})
        );
        ops.push(
          this.addressClusterTable.putOperation({address: address}, {clusterId: toClusterId})
        );
        nextIndex++;
      });
      ops.push(
        this.clusterAddressCountTable.delOperation({clusterId: fromClusterId})
      );
    });
    if (nonClusterAddresses !== undefined && nonClusterAddresses.length > 0) {
      let addAddressesOps = await this.addAddressesToClusterOps(nonClusterAddresses, toClusterId, nextIndex);
      addAddressesOps.forEach(op => ops.push(op));
      nextIndex += nonClusterAddresses.length;
    }
    ops.push(
      this.clusterAddressCountTable.putOperation({clusterId: toClusterId}, {addressCount: nextIndex})
    );
    return ops;
  }

  async mergeClusterAddresses(toClusterId: number, ...fromClusterIds: number[]) {
    return this.db.batchBinary(await this.mergeClusterAddressesOps(toClusterId, fromClusterIds));
  }

  async addAddressesToClusterOps(addresses: string[], clusterId: number, oldClusterAddressCount?: number): Promise<AbstractBatch<Buffer, Buffer>[]> {
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    if (oldClusterAddressCount === undefined) {
      oldClusterAddressCount = (await this.clusterAddressCountTable.get({clusterId: clusterId})).addressCount;
    }
    addresses.forEach((address, index) => {
      let newIndex: number = Number(oldClusterAddressCount)+index;
      ops.push(this.clusterAddressTable.putOperation({clusterId: clusterId, addressIndex: newIndex}, {address: address}));
      ops.push(this.addressClusterTable.putOperation({address: address}, {clusterId: clusterId}));
    });
    return ops;
  }

  async addAddressesToCluster(addresses: string[], clusterId: number): Promise<void> {
    return this.db.batchBinary(await this.addAddressesToClusterOps(addresses, clusterId));
  }

  async createAddressClustersOps(clusterAddresses: string[], clusterId: number): Promise<AbstractBatch<Buffer, Buffer>[]> {
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    if (clusterAddresses.length === 0) throw new Error("createAddressClustersOps called with 0 addresses");
    ops.push(
      this.clusterAddressCountTable.putOperation({clusterId: clusterId}, {addressCount: clusterAddresses.length})
    );
    clusterAddresses.forEach((address: string, index: number) => {
      ops.push(
        this.clusterAddressTable.putOperation({clusterId, addressIndex: index}, {address: address})
      );
      ops.push(
        this.addressClusterTable.putOperation({address: address}, {clusterId: clusterId})
      );
    });
    return ops;
  }  

  async createMultipleAddressClusters(clusters: Array<string[]>, next_cluster_id?: number) {
    if (next_cluster_id === undefined) {
      try {
        next_cluster_id = (await this.nextClusterIdTable.get(undefined)).nextClusterId;
      } catch (error) {
        next_cluster_id = 0;
      }
    }
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    for (let clusterAddresses of clusters) {
      let newOps = await this.createAddressClustersOps(clusterAddresses, next_cluster_id);
      newOps.forEach(op => ops.push(op));
      next_cluster_id++;
    }
    ops.push(this.nextClusterIdTable.putOperation(undefined, {nextClusterId: next_cluster_id}));
    return this.db.batchBinary(ops);
  }

  async createClusterWithAddresses(addresses: string[]): Promise<void> {
    return this.createMultipleAddressClusters([addresses]);
  }

}  