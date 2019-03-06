import { ClusterAddress } from '../models/cluster-address';
import { AddressBalanceTable } from '../tables/address-balance-table';
import { AddressClusterTable } from '../tables/address-cluster-table';
import { AddressTransactionTable } from '../tables/address-transaction-table';
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
  addressBalanceTable: AddressBalanceTable;
  addressTransactionTable: AddressTransactionTable;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService) {
    this.clusterAddressTable = new ClusterAddressTable(db, addressEncodingService);
    this.clusterAddressCountTable = new ClusterAddressCountTable(db);
    this.nextClusterIdTable = new NextClusterIdTable(db);
    this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
    this.addressBalanceTable = new AddressBalanceTable(db, addressEncodingService);
    this.addressTransactionTable = new AddressTransactionTable(db, addressEncodingService);
  }  

  async getAddressBalance(address: string): Promise<number> {
    try {
      return (await this.addressBalanceTable.get({address: address})).balance;
    } catch(err) {
      if (err.notFound) {
        return undefined;
      }
      throw err;
    }  
  }

  async getAddressCluster(address: string): Promise<number> {
    return (await this.addressClusterTable.get({address: address})).clusterId;
  }

  async getAddressCount(clusterId: number): Promise<number> {
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
    let promises = [];
    let addresses = [];
    addressToDelta.forEach((delta: number, address: string) => {
      addresses.push(address);
      promises.push(this.addressClusterTable.get({address: address}));
    });
    let clusterIds = await Promise.all(promises);
    let clusterToDelta = new Map<number, number>();
    addresses.forEach((address: string, index: number) => {
      let clusterId: number = clusterIds[index].clusterId;
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
    let promises: Promise<any>[] = [];
    promises.push(this.getAddressCount(toClusterId));
    fromClusterIds.forEach(fromClusterId => {
      promises.push(this.getClusterAddresses(fromClusterId));
    });
    let values = await Promise.all(promises);
    let nextIndex = values[0];
    for (let clusterIndex = 0; clusterIndex < fromClusterIds.length; clusterIndex++) {
      let fromClusterId = fromClusterIds[clusterIndex];
      let addresses: ClusterAddress[] = values[1+clusterIndex];
      for (let addressIndex = 0; addressIndex < addresses.length; addressIndex++) {
        let address = addresses[addressIndex];
        await this.db.writeBatchService.push(
          this.clusterAddressTable.putOperation({clusterId: toClusterId, balance: address.balance, address: address.address}, {})
          //this.clusterAddressTable.putOperation({clusterId: toClusterId, addressIndex: nextIndex}, {address: address})
        );
        await this.db.writeBatchService.push(
          this.clusterAddressTable.delOperation({clusterId: fromClusterId, balance: address.balance, address: address.address})
          //this.clusterAddressTable.delOperation({clusterId: fromClusterId, addressIndex: addressIndex})
        );
        await this.db.writeBatchService.push(
          this.addressClusterTable.putOperation({address: address.address}, {clusterId: toClusterId})
          //this.addressClusterTable.putOperation({address: address}, {clusterId: toClusterId})
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
      oldClusterAddressCount = await this.getAddressCount(clusterId);
    }
    for (const [index, address] of addresses.entries()) {
      let newIndex: number = Number(oldClusterAddressCount)+index;
      await this.db.writeBatchService.push(this.clusterAddressTable.putOperation({clusterId: clusterId, balance: 0, address: address}, {}));
      //await this.db.writeBatchService.push(this.clusterAddressTable.putOperation({clusterId: clusterId, addressIndex: newIndex}, {address: address}));
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
        this.clusterAddressTable.putOperation({clusterId, balance: 0, address: address}, {})
        //this.clusterAddressTable.putOperation({clusterId, addressIndex: index}, {address: address})
      );
      await this.db.writeBatchService.push(
        this.addressClusterTable.putOperation({address: address}, {clusterId: clusterId})
      );
    };
  }  

}  