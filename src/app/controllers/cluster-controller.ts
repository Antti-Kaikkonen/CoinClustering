import { Request, Response } from 'express';
import { Transaction } from '../models/transaction';
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { ClusterAddressService } from '../services/cluster-address-service';
import { ClusterTransactionService } from '../services/cluster-transaction-service';
import { AddressClusterTable } from '../tables/address-cluster-table';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterMergedToTable } from '../tables/cluster-merged-to-table';
import RpcApi from '../utils/rpc-api';
import { txAddressBalanceChanges } from '../utils/utils';


export class ClusterController {

  private clusterTransactionService: ClusterTransactionService;
  private clusterAddressService: ClusterAddressService;
  private balanceToClusterTable: BalanceToClusterTable;
  private addressClusterTable: AddressClusterTable;
  private clusterMergedToTable: ClusterMergedToTable;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService, private rpcApi: RpcApi) {
    this.clusterTransactionService = new ClusterTransactionService(db);
    this.clusterAddressService = new ClusterAddressService(db, addressEncodingService);
    this.balanceToClusterTable = new BalanceToClusterTable(db);
    this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
    this.clusterMergedToTable = new ClusterMergedToTable(db);
  }  


  private async redirectToCluster(clusterId: number): Promise<number> {
    try {
      return (await this.clusterMergedToTable.get({fromClusterId: clusterId})).toClusterId;
    } catch(err) {
      if (!err.notFound) throw err;
    } 
    return undefined;
  }

  clusterTransactions = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);
    let redirectToCluster = await this.redirectToCluster(clusterId);
    if (redirectToCluster !== undefined) {
      let newPath = req.baseUrl+req.route.path.replace(':id', redirectToCluster);
      res.redirect(301, newPath);
    } else {
      let result = await this.clusterTransactionService.getClusterTransactions(clusterId);
      res.send(result);
    }
  };

  clusterAddresses = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);
    let redirectToCluster = await this.redirectToCluster(clusterId);
    if (redirectToCluster !== undefined) {
      let newPath = req.baseUrl+req.route.path.replace(':id', redirectToCluster);
      res.redirect(301, newPath);
    } else {
      let result = await this.clusterAddressService.getClusterAddresses(clusterId);
      res.send(result);
    }  
  };

  private async addressBalanceChangesToClusterBalanceChanges(addressToDelta: Map<string, number>): Promise<Map<number, number>> {
    let promises = [];
    let addresses = [];
    addressToDelta.forEach((delta: number, address: string) => {
      addresses.push(address);
      promises.push(this.addressClusterTable.get({address: address}));
      //promises.push(this.db.get(db_address_cluster_prefix+address));
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

  txClusterBalnaceChanges = async (req:Request, res:Response) => {
    let txid: string = req.params.txid;
    let tx: Transaction = (await this.rpcApi.getTransactions([txid]))[0];
    let txids: Set<string> = new Set();
    tx.vin.forEach(vin => {
      if (vin.coinbase) return;
      if (!vin.address) {
        txids.add(vin.txid);
      }
    });
    let txs = await this.rpcApi.getTransactions(Array.from(txids));
    let txidToTx: Map<string, Transaction> = new Map();
    txs.forEach(tx => txidToTx.set(tx.txid, tx));
    tx.vin.forEach(vin => {
      if (vin.coinbase) return;
      if (!vin.address) {
        let vout = txidToTx.get(vin.txid).vout[vin.vout];
        if (vout.scriptPubKey.addresses.length === 1) {
          vin.address = vout.scriptPubKey.addresses[0];
        }
        vin.value = vout.value;
      }
    });
    let balanceChanges: Map<string, number> = txAddressBalanceChanges(tx);

    let clusterBalanceChanges = await this.addressBalanceChangesToClusterBalanceChanges(balanceChanges);
    //console.log("clusterBalanceChanges", clusterBalanceChanges);
    let result = {};
    clusterBalanceChanges.forEach((delta: number, clusterId: number) => {
      result[clusterId] = delta;
    });
    res.send(result);
  }  


  largestClusters = async (req:Request, res:Response) => {
    res.contentType('application/json');
    let n: number = Number(req.params.n);
    let result = [];
    res.write('[');
    let first = true;
    let stream = this.balanceToClusterTable.createReadStream({reverse: true, limit: n})
    .on('data', (data) => {
      if (!first) res.write(",");
      res.write(JSON.stringify({
        clusterId: data.key.clusterId,
        balance: data.key.balance
      }));
      first = false;
    }).on('finish', () => {
      res.write(']');
      res.end();
    });
    req.on('close', () => {
      console.log("cancelled by user. destroying");
      stream['destroy']();
      console.log("destroyed");
    });
  }

}  