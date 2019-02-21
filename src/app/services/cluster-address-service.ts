import { AddressClusterTable } from '../tables/address-cluster-table';
import { ClusterAddressCountTable } from '../tables/cluster-address-count-table';
import { ClusterAddressTable } from '../tables/cluster-address-table';
import { NextClusterIdTable } from '../tables/next-cluster-id-table';
import { AddressEncodingService } from './address-encoding-service';
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
      })
      .on('end', function () {
        resolve(addresses);
      });
    });
  }

  async mergeClusterAddressesOps(toClusterId: number, fromClusterIds: number[], nonClusterAddresses?: string[]): Promise<void> {
    if (fromClusterIds.length === 0 && (nonClusterAddresses === undefined || nonClusterAddresses.length === 0)) return;
    let promises: Promise<any>[] = [];
    promises.push(this.clusterAddressCountTable.get({clusterId: toClusterId}));

    fromClusterIds.forEach(fromClusterId => {
      promises.push(this.getClusterAddresses(fromClusterId));
    });
    let values
    try {
      values = await Promise.all(promises);
    } catch(err) {
      if (err.notFound) console.log("Failed to get ", err.originalKey);
      throw err;
    }
    let nextIndex = values[0].addressCount;

    for (let clusterIndex = 0; clusterIndex < fromClusterIds.length; clusterIndex++) {
      let fromClusterId = fromClusterIds[clusterIndex];
      let addresses: string[] = values[1+clusterIndex];
      for (let addressIndex = 0; addressIndex < addresses.length; addressIndex++) {
        let address = addresses[addressIndex];
        await this.db.writeBatchService.push(
          this.clusterAddressTable.putOperation({clusterId: toClusterId, addressIndex: nextIndex}, {address: address})
        );
        await this.db.writeBatchService.push(
          this.clusterAddressTable.delOperation({clusterId: fromClusterId, addressIndex: addressIndex})
        );
        await this.db.writeBatchService.push(
          this.addressClusterTable.putOperation({address: address}, {clusterId: toClusterId})
        );
        nextIndex++;
      };
      await this.db.writeBatchService.push(
        this.clusterAddressCountTable.delOperation({clusterId: fromClusterId})
      );
    };
    if (nonClusterAddresses !== undefined && nonClusterAddresses.length > 0) {
      await this.addAddressesToClusterOps(nonClusterAddresses, toClusterId, nextIndex);
      nextIndex += nonClusterAddresses.length;
    }
    await this.db.writeBatchService.push(
      this.clusterAddressCountTable.putOperation({clusterId: toClusterId}, {addressCount: nextIndex})
    );
  }

  async mergeClusterAddresses(toClusterId: number, ...fromClusterIds: number[]) {
    await this.mergeClusterAddressesOps(toClusterId, fromClusterIds);
    await this.db.writeBatchService.commit();
  }

  async addAddressesToClusterOps(addresses: string[], clusterId: number, oldClusterAddressCount?: number): Promise<void> {
    if (oldClusterAddressCount === undefined) {
      oldClusterAddressCount = (await this.clusterAddressCountTable.get({clusterId: clusterId})).addressCount;
    }
    for (const [index, address] of addresses.entries()) {
      let newIndex: number = Number(oldClusterAddressCount)+index;
      await this.db.writeBatchService.push(this.clusterAddressTable.putOperation({clusterId: clusterId, addressIndex: newIndex}, {address: address}));
      await this.db.writeBatchService.push(this.addressClusterTable.putOperation({address: address}, {clusterId: clusterId}));
    };
  }

  async addAddressesToCluster(addresses: string[], clusterId: number): Promise<void> {
    await this.addAddressesToClusterOps(addresses, clusterId)
    return this.db.writeBatchService.commit();
  }

  async createAddressClustersOps(clusterAddresses: string[], clusterId: number): Promise<void> {
    if (clusterAddresses.length === 0) throw new Error("createAddressClustersOps called with 0 addresses");
    await this.db.writeBatchService.push(
      this.clusterAddressCountTable.putOperation({clusterId: clusterId}, {addressCount: clusterAddresses.length})
    );
    for (const [index, address] of clusterAddresses.entries()) {
      await this.db.writeBatchService.push(
        this.clusterAddressTable.putOperation({clusterId, addressIndex: index}, {address: address})
      );
      await this.db.writeBatchService.push(
        this.addressClusterTable.putOperation({address: address}, {clusterId: clusterId})
      );
    };
  }  

}  