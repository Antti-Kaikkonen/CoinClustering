import { Cluster } from "../models/cluster";
import { ClusterId } from "../models/clusterid";
import { Transaction, TransactionOutput } from "../models/transaction";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { txAddresses, txAddressesToCluster } from "./utils";

export class ClusterBuilder {

  constructor(private addressClusterTable: AddressClusterTable) {

  }

  addressToClusterPromise: Map<string, Promise<ClusterId>> = new Map();
  addressToClusterId: Map<string, ClusterId> = new Map();

  clusterIdToCluster: Map<string, Cluster> = new Map();//key=clusterId.toString();
  clusterAddressToCluster: Map<string, Cluster> = new Map();

  addressToFirstOutput: Map<string, ClusterId> = new Map();

  txProcessedPromises = [];

  private async getClusterId(address: string): Promise<ClusterId> {
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
    fromCluster.clusterIds.forEach(clusterId => this.clusterIdToCluster.set(clusterId.toString(), toCluster));
    fromCluster.addresses.forEach(address => this.clusterAddressToCluster.set(address, toCluster));
  }

  private onTxClusterIdsResolved(tx: Transaction, height: number, txN: number) {
    let newCluster: Cluster = new Cluster([], []);
    //console.log("onTxClusterIdsResolved", height, txN);
    /*tx.vout.forEach((vout:TransactionOutput) => {
      if (vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1 && vout.scriptPubKey.addresses[0]) {
        let address = vout.scriptPubKey.addresses[0];
        let clusterId = this.addressToClusterId.get(address);
        if (clusterId === undefined) {
          let clusterId = new ClusterId(height, txN, vout.n);
          this.clusterAddressToCluster.set(address, new Cluster([address], [], clusterId));
        }  
      }
    });*/

    let txAddrToCluster = txAddressesToCluster(tx);


    let txAddr = txAddresses(tx);
    let txAddrArray = Array.from(txAddr);
    //txAddrArray.sort();
    txAddrArray.forEach(address => {
      let shouldCluster = txAddrToCluster.has(address);
      let clusterId = this.addressToClusterId.get(address);
      if (clusterId !== undefined) {
        if (!shouldCluster) return;
        let intersectingCluster = this.clusterIdToCluster.get(clusterId.toString());
        if (intersectingCluster !== undefined) {
          if (intersectingCluster === newCluster) return;
          this.mergeClusters(intersectingCluster, newCluster);
        } else {
          newCluster.clusterIds.push(clusterId);
          this.clusterIdToCluster.set(clusterId.toString(), newCluster);
        }
      } else {
        let intersectingCluster = this.clusterAddressToCluster.get(address);
        if (intersectingCluster !== undefined) {
          if (intersectingCluster === newCluster) return;
          if (!shouldCluster) return;
          this.mergeClusters(intersectingCluster, newCluster);
        } else {
          if (!shouldCluster) {
            //let ci = this.addressToFirstOutput.get(address);
            //throw new Error("gdflkjgldfkjglkdf"+height+","+txN+","+address);//TODO: Fix when mixing transaction input is evaluated before the spent output transaction
            this.clusterAddressToCluster.set(address, new Cluster([address], [], this.addressToFirstOutput.get(address)));
            //console.log("this.addressToFirstOutput.get(address)", this.addressToFirstOutput.get(address));
          } else {
            newCluster.addresses.push(address);
            let ci = this.addressToFirstOutput.get(address);
            newCluster.updateLowestClusterId(ci);
            this.clusterAddressToCluster.set(address, newCluster);
          }
        }
      }
    });
  }

  add(tx:Transaction, height: number, txN: number) {

    let txAddr = txAddresses(tx);

    tx.vout.forEach((vout:TransactionOutput) => {
      if (vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1 && vout.scriptPubKey.addresses[0]) {
        let address = vout.scriptPubKey.addresses[0];
        let clusterId = new ClusterId(height, txN, vout.n);
        if (!this.addressToFirstOutput.has(address)) this.addressToFirstOutput.set(address, clusterId);
      }
    });

    let txProcessedPromise = new Promise((resolve, reject) => {
      let addressesToResolve: number = txAddr.size;
      if(addressesToResolve === 0) {
        /*console.log("tx has no addresses?"+ JSON.stringify(tx));*/
        resolve();
      }
      txAddr.forEach(address => {
        let promise = this.addressToClusterPromise.get(address);
        if (!promise) {
          promise = this.getClusterId(address);
          this.addressToClusterPromise.set(address, promise);
        }  
        promise.then((clusterId: ClusterId) => {
          this.addressToClusterId.set(address, clusterId);
          addressesToResolve--;
          if (addressesToResolve === 0) {
            this.onTxClusterIdsResolved(tx, height, txN);
            resolve();
            return;
          }
        }, (err) => {console.log("got err", err)});
      });
    });
    this.txProcessedPromises.push(txProcessedPromise);
  }

  async build(): Promise<Cluster[]> {
    await Promise.all(this.txProcessedPromises);
    let clusters: Set<Cluster> = new Set([...this.clusterIdToCluster.values(), ...this.clusterAddressToCluster.values()]);
    return Array.from(clusters);
  }
  
}