import { injectable } from 'inversify';
import { Writable } from 'stream';
import { ClusterAddress } from '../models/cluster-address';
import { ClusterId } from '../models/clusterid';
import { AddressClusterTable } from '../tables/address-cluster-table';
import { ClusterAddressCountTable } from '../tables/cluster-address-count-table';
import { ClusterAddressTable } from '../tables/cluster-address-table';
import { AddressService } from './address-service';
import { BinaryDB } from './binary-db';

@injectable()
export class ClusterAddressService {

  constructor(private db: BinaryDB,  
    private addressService: AddressService,
    private clusterAddressTable: ClusterAddressTable,
    private clusterAddressCountTable: ClusterAddressCountTable,
    private addressClusterTable: AddressClusterTable,
  ) {}  

  async getAddressCountDefaultUndefined(clusterId: ClusterId): Promise<number> {
    try {
      return (await this.clusterAddressCountTable.get({clusterId: clusterId})).addressCount;
    } catch(err) {
      if (err.notFound) {
        return undefined;
      }
      throw err;
    }
  }

  async addressBalanceChangesToClusterBalanceChanges(addressToDelta: Map<string, number>): Promise<Map<string, number>> {
    let clusterIdPromises: Promise<ClusterId>[] = [];
    let addresses: string[] = [];
    addressToDelta.forEach((delta: number, address: string) => {
      addresses.push(address);
      clusterIdPromises.push(this.addressService.getAddressCluster(address));
    });
    let clusterIds: ClusterId[] = await Promise.all(clusterIdPromises);
    let clusterToDelta = new Map<string, number>();
    addresses.forEach((address: string, index: number) => {
      let clusterId: ClusterId = clusterIds[index];
      if (clusterId === undefined) throw Error("Cluster missing");
      let oldBalance = clusterToDelta.get(clusterId.toString());
      let addressDelta = addressToDelta.get(address);
      if (!oldBalance) oldBalance = 0;
      clusterToDelta.set(clusterId.toString(), oldBalance+addressDelta);
    });
    return clusterToDelta;
  }

  async getClusterAddresses(clusterId: ClusterId): Promise<ClusterAddress[]> {
    return new Promise<ClusterAddress[]>((resolve, reject) => {
      let addresses: ClusterAddress[] = [];
      let nextCluster = new ClusterId(clusterId.height, clusterId.txN, clusterId.outputN+1);
      this.clusterAddressTable.createReadStream({
        gte: {clusterId: clusterId},
        lt: {clusterId: nextCluster}
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

  async mergeClusterAddressesOps(toClusterId: ClusterId, fromClusterIds: ClusterId[], nonClusterAddresses?: string[]): Promise<void> {
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
        let nextClusterId = new ClusterId(fromClusterId.height, fromClusterId.txN, fromClusterId.outputN+1);
        this.clusterAddressTable.createReadStream({
          gte: {clusterId: fromClusterId},
          lt: {clusterId: nextClusterId}
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

  async addAddressesToClusterOps(addresses: string[], clusterId: ClusterId): Promise<void> {
    for (const [index, address] of addresses.entries()) {
      await this.db.writeBatchService.push(this.clusterAddressTable.putOperation({clusterId: clusterId, balance: 0, address: address}, {}));
      await this.db.writeBatchService.push(this.addressClusterTable.putOperation({address: address}, {clusterId: clusterId}));
    }
  }

  async createAddressClustersOps(clusterAddresses: string[], clusterId: ClusterId): Promise<void> {
    if (clusterAddresses.length === 0) throw new Error("createAddressClustersOps called with 0 addresses");
    //console.log("createAddressClustersOps", clusterAddresses, clusterId);
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
    }
  }  

}  