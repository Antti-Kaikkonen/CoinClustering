import { Cluster } from "../models/cluster";
import { Transaction } from "../models/transaction";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { txAddresses, txAddressesToCluster } from "../utils/utils";

export class ClusterBuilder {

  constructor(private addressClusterTable: AddressClusterTable) {

  }

  addressToClusterPromise: Map<string, Promise<number>> = new Map();

  clusterIdToCluster: Map<number, Cluster> = new Map();
  clusterAddressToCluster: Map<string, Cluster> = new Map();

  txProcessedPromises: Promise<void>[] = [];

  private async getClusterId(address: string): Promise<number> {
    try {
      return (await this.addressClusterTable.get({address: address})).clusterId;
    } catch(err) {
      if (err.notFound)
        return undefined;
      throw err;
    }
  }

  private mergeClusters(fromCluster: Cluster, toCluster: Cluster) {
    toCluster.mergeFrom(fromCluster);
    fromCluster.clusterIds.forEach(clusterId => this.clusterIdToCluster.set(clusterId, toCluster));
    fromCluster.addresses.forEach(address => this.clusterAddressToCluster.set(address, toCluster));
  }

  private async onTxClusterIdsResolved(tx: Transaction) {
    let newCluster: Cluster = new Cluster();
    let txAddrToCluster = txAddressesToCluster(tx);
    let txAddr = txAddresses(tx);
    for (let address of txAddr) {
      let shouldCluster = txAddrToCluster.has(address);
      let clusterId = (await this.addressToClusterPromise.get(address));
      if (clusterId !== undefined) {
        if (!shouldCluster) continue;
        let intersectingCluster = this.clusterIdToCluster.get(clusterId);
        if (intersectingCluster !== undefined) {
          if (intersectingCluster === newCluster) continue;
          this.mergeClusters(intersectingCluster, newCluster);
        } else {
          newCluster.clusterIds.add(clusterId);
          this.clusterIdToCluster.set(clusterId, newCluster);
        }
      } else {
        let intersectingCluster = this.clusterAddressToCluster.get(address);
        if (intersectingCluster !== undefined) {
          if (intersectingCluster === newCluster) continue;
          if (!shouldCluster) continue;
          this.mergeClusters(intersectingCluster, newCluster);
        } else {
          if (!shouldCluster) {
            this.clusterAddressToCluster.set(address, new Cluster(new Set([address])));
          } else {
            newCluster.addresses.add(address);
            this.clusterAddressToCluster.set(address, newCluster);
          }
        }  
      }
    }
  }

  add(tx:Transaction) {
    let txAddr = txAddresses(tx);


    let clusterIdPromises = [];
    txAddr.forEach(address => {
      let oldPromise = this.addressToClusterPromise.get(address);
      if (!oldPromise) {
        let promise = this.getClusterId(address);
        this.addressToClusterPromise.set(address, promise);
        clusterIdPromises.push(promise);
      } else {
        clusterIdPromises.push(oldPromise);
      }
    });
    let txProcessedPromise = new Promise<void>((resolve, reject) => {
      Promise.all(clusterIdPromises).then(() => {
        this.onTxClusterIdsResolved(tx).then(() => resolve());
      })
    });
    this.txProcessedPromises.push(txProcessedPromise);

  }

  async build(): Promise<Cluster[]> {
    await Promise.all(this.txProcessedPromises);
    let clusters: Set<Cluster> = new Set([...this.clusterIdToCluster.values(), ...this.clusterAddressToCluster.values()]);
    return Array.from(clusters);
  }
}