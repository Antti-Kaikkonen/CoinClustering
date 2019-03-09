import { Writable } from 'stream';
import { ClusterAddress } from '../models/cluster-address';
import { AddressBalanceTable } from '../tables/address-balance-table';
import { AddressClusterTable } from '../tables/address-cluster-table';
import { AddressTransactionTable } from '../tables/address-transaction-table';
import { ClusterAddressCountTable } from '../tables/cluster-address-count-table';
import { ClusterAddressTable } from '../tables/cluster-address-table';
import { NextClusterIdTable } from '../tables/next-cluster-id-table';
import { AddressEncodingService } from './address-encoding-service';
import { AddressService } from './address-service';
import { BinaryDB } from './binary-db';

export class ClusterAddressService {

  clusterAddressTable: ClusterAddressTable;
  clusterAddressCountTable: ClusterAddressCountTable;
  nextClusterIdTable: NextClusterIdTable;
  addressClusterTable: AddressClusterTable;
  addressBalanceTable: AddressBalanceTable;
  addressTransactionTable: AddressTransactionTable;

  constructor(private db: BinaryDB,  addressEncodingService: AddressEncodingService, private addressService: AddressService) {
    this.clusterAddressTable = new ClusterAddressTable(db, addressEncodingService);
    this.clusterAddressCountTable = new ClusterAddressCountTable(db);
    this.nextClusterIdTable = new NextClusterIdTable(db);
    this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
    this.addressBalanceTable = new AddressBalanceTable(db, addressEncodingService);
    this.addressTransactionTable = new AddressTransactionTable(db, addressEncodingService);
  }  

  async getAddressCountDefaultUndefined(clusterId: number): Promise<number> {
    try {
      return (await this.clusterAddressCountTable.get({clusterId: clusterId})).addressCount;
    } catch(err) {
      if (err.notFound) {
        return undefined;
      }
      throw err;
    }
  }

  async addressBalanceChangesToClusterBalanceChanges(addressToDelta: Map<string, number>): Promise<Map<number, number>> {
    let clusterIdPromises: Promise<number>[] = [];
    let addresses: string[] = [];
    addressToDelta.forEach((delta: number, address: string) => {
      addresses.push(address);
      clusterIdPromises.push(this.addressService.getAddressCluster(address));
    });
    let clusterIds: number[] = await Promise.all(clusterIdPromises);
    let clusterToDelta = new Map<number, number>();
    addresses.forEach((address: string, index: number) => {
      let clusterId: number = clusterIds[index];
      if (clusterId === undefined) throw Error("Cluster missing");
      let oldBalance = clusterToDelta.get(clusterId);
      let addressDelta = addressToDelta.get(address);
      if (!oldBalance) oldBalance = 0;
      clusterToDelta.set(clusterId, oldBalance+addressDelta);
    });
    return clusterToDelta;
  }

  async getClusterAddresses(clusterId: number): Promise<ClusterAddress[]> {
    return new Promise<ClusterAddress[]>((resolve, reject) => {
      let addresses: ClusterAddress[] = [];
      this.clusterAddressTable.createReadStream({
        gte: {clusterId: clusterId},
        lt: {clusterId: clusterId+1}
      }).on('data', function (data) {
        let ca = new ClusterAddress(data.key.balance, data.key.address);
        addresses.push(ca);
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
    let oldAddressCountPromise: Promise<number> = this.getAddressCountDefaultUndefined(toClusterId);
    let newAddressesCount = 0;
    let allClustersMerged = new Promise((resolve, reject) => {
      let clustersToMerge: number = fromClusterIds.length;
      if (clustersToMerge === 0) resolve();
      fromClusterIds.forEach(fromClusterId => {
        let addressMerger = new Writable({
          objectMode: true,
          write: async (data: {key: {clusterId: number, balance: number, address: string}, value: {}}, encoding, callback) => {
            newAddressesCount++;
            let address = new ClusterAddress(data.key.balance, data.key.address);
            await this.db.writeBatchService.push(
              this.clusterAddressTable.putOperation({clusterId: toClusterId, balance: address.balance, address: address.address}, {})
            );
            await this.db.writeBatchService.push(
              this.clusterAddressTable.delOperation({clusterId: fromClusterId, balance: address.balance, address: address.address})
            );
            await this.db.writeBatchService.push(
              this.addressClusterTable.putOperation({address: address.address}, {clusterId: toClusterId})
            );
            callback(null);
          }
        });
        this.clusterAddressTable.createReadStream({
          gte: {clusterId: fromClusterId},
          lt: {clusterId: fromClusterId+1}
        }).pipe(addressMerger);

        addressMerger.on('finish', async () => {
          await this.db.writeBatchService.push(
            this.clusterAddressCountTable.delOperation({clusterId: fromClusterId})
          );
          clustersToMerge--;
          if (clustersToMerge === 0) resolve();
        });
      });
    });
    await allClustersMerged;
    if (nonClusterAddresses !== undefined && nonClusterAddresses.length > 0) {
      await this.addAddressesToClusterOps(nonClusterAddresses, toClusterId);
      newAddressesCount += nonClusterAddresses.length;
    }
    newAddressesCount += await oldAddressCountPromise;
    await this.db.writeBatchService.push(
      this.clusterAddressCountTable.putOperation({clusterId: toClusterId}, {addressCount: newAddressesCount})
    );
  }

  async addAddressesToClusterOps(addresses: string[], clusterId: number): Promise<void> {
    for (const [index, address] of addresses.entries()) {
      await this.db.writeBatchService.push(this.clusterAddressTable.putOperation({clusterId: clusterId, balance: 0, address: address}, {}));
      await this.db.writeBatchService.push(this.addressClusterTable.putOperation({address: address}, {clusterId: clusterId}));
    };
  }

  async createAddressClustersOps(clusterAddresses: string[], clusterId: number): Promise<void> {
    if (clusterAddresses.length === 0) throw new Error("createAddressClustersOps called with 0 addresses");
    await this.db.writeBatchService.push(
      this.clusterAddressCountTable.putOperation({clusterId: clusterId}, {addressCount: clusterAddresses.length})
    );
    for (const [index, address] of clusterAddresses.entries()) {
      await this.db.writeBatchService.push(
        this.clusterAddressTable.putOperation({clusterId, balance: 0, address: address}, {})
      );
      await this.db.writeBatchService.push(
        this.addressClusterTable.putOperation({address: address}, {clusterId: clusterId})
      );
    };
  }  

}  