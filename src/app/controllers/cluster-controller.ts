import { Request, Response } from 'express';
import RpcApi from '../misc/rpc-api';
import { txAddressBalanceChanges } from '../misc/utils';
import { ClusterTransaction } from '../models/cluster-transaction';
import { Transaction } from '../models/transaction';
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { ClusterAddressService } from '../services/cluster-address-service';
import { ClusterTransactionService } from '../services/cluster-transaction-service';
import { AddressClusterTable } from '../tables/address-cluster-table';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterAddressTable } from '../tables/cluster-address-table';
import { ClusterMergedToTable } from '../tables/cluster-merged-to-table';
import { ClusterTransactionTable } from '../tables/cluster-transaction-table';


export class ClusterController {

  private clusterTransactionService: ClusterTransactionService;
  private clusterAddressService: ClusterAddressService;
  private balanceToClusterTable: BalanceToClusterTable;
  private addressClusterTable: AddressClusterTable;
  private clusterMergedToTable: ClusterMergedToTable;
  private clusterTransactionTable: ClusterTransactionTable;
  private clusterAddressTable: ClusterAddressTable;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService, private rpcApi: RpcApi) {
    this.clusterTransactionService = new ClusterTransactionService(db);
    this.clusterAddressService = new ClusterAddressService(db, addressEncodingService);
    this.balanceToClusterTable = new BalanceToClusterTable(db);
    this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
    this.clusterMergedToTable = new ClusterMergedToTable(db);
    this.clusterTransactionTable = new ClusterTransactionTable(db);
    this.clusterAddressTable = new ClusterAddressTable(db, addressEncodingService);
  }  


  private async redirectToCluster(clusterId: number): Promise<number> {
    try {
      return (await this.clusterMergedToTable.get({fromClusterId: clusterId})).toClusterId;
    } catch(err) {
      if (!err.notFound) throw err;
    } 
    return undefined;
  }

  clusterInfo = async (req: Request, res: Response) => {
    let clusterId: number = Number(req.params.id);
    let redirectToCluster = await this.redirectToCluster(clusterId);
    if (redirectToCluster !== undefined) {
      let newPath = req.baseUrl+req.route.path.replace(':id', redirectToCluster);
      res.redirect(301, newPath);
    } else {
      let addressCountPromise = this.clusterAddressService.getAddressCount(clusterId);
      let balancePromise = this.clusterTransactionService.getClusterBalanceWithUndefined(clusterId);
      let firstTransactionPromise = this.clusterTransactionService.getFirstTransaction(clusterId);
      let lastTransactionPromise = this.clusterTransactionService.getLastClusetrTransaction(clusterId);
      let addressCount = await addressCountPromise;
      if (addressCount === undefined) {
        res.sendStatus(404);
      } else {
        res.send({
          balance: await balancePromise,
          firstTransaction: await firstTransactionPromise,
          lastTransaction: await lastTransactionPromise,
          addressCount: await addressCountPromise
        });
      }
    }  
  }

  clusterTransactions = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);

    let limit = req.query.limit !== undefined ? Number(req.query.limit) : 100;
    if (!Number.isInteger(limit) || limit <= 0 || limit > 1000) {
      res.sendStatus(400);
      return;
    }

    let afterBlock: number;
    let afterTxN: number;
    if (req.query.after !== undefined) {
      let afterComponents = req.query.after.split('-');
      if (afterComponents.length > 2) {
        res.sendStatus(400);
        return;
      }
      afterBlock = Number(afterComponents[0]);
      if (!Number.isInteger(afterBlock) || afterBlock < 0) {
        res.sendStatus(400);
        return;
      }
      if (afterComponents.length === 2) {
        afterTxN = Number(afterComponents[1]);
        if (!Number.isInteger(afterTxN) || afterTxN < 0) {
          res.sendStatus(400);
          return;
        }
      }
    }

    let redirectToCluster = await this.redirectToCluster(clusterId);
    if (redirectToCluster !== undefined) {
      let newPath = req.baseUrl+req.route.path.replace(':id', redirectToCluster);
      res.redirect(301, newPath);
    } else {
      res.contentType('application/json');
      res.write('[');
      let first = true;
      let stream = this.clusterTransactionTable.createReadStream({
        gt: {clusterId: clusterId, height: afterBlock, n: afterTxN},
        lt: {clusterId: clusterId+1},
        limit: limit
      });
      stream.on('data', (data) => {
        if (!first) res.write(",");
        res.write(JSON.stringify(new ClusterTransaction(
          data.value.txid,
          data.key.height,
          data.key.n
        )));
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
  };

  clusterAddresses = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);

    let limit = req.query.limit !== undefined ? Number(req.query.limit) : 100;
    if (!Number.isInteger(limit) || limit <= 0 || limit > 1000) {
      res.sendStatus(400);
      return;
    }

    let afterBalance: number;
    let afterAdddress: string;
    if (req.query.after !== undefined) {
      let afterComponents = req.query.after.split('-');
      if (afterComponents.length > 2) {
        res.sendStatus(400);
        return;
      }
      afterBalance = Number(afterComponents[0]);
      if (!Number.isInteger(afterBalance) || afterBalance < 0) {
        res.sendStatus(400);
        return;
      }
      if (afterComponents.length === 2) {
        afterAdddress = afterComponents[1];
        if (afterAdddress.length === 0) {
          res.sendStatus(400);
          return;
        }
      }
    }

    let redirectToCluster = await this.redirectToCluster(clusterId);
    if (redirectToCluster !== undefined) {
      let newPath = req.baseUrl+req.route.path.replace(':id', redirectToCluster);
      res.redirect(301, newPath);
    } else {
      let lt;
      if (afterBalance !== undefined) {
        lt = {clusterId: clusterId, balance: afterBalance, address: afterAdddress};
      } else {
        lt = {clusterId: clusterId+1};
      }
      console.log("lt", lt);

      res.contentType('application/json');
      res.write('[');
      let first = true;
    
      let stream = this.clusterAddressTable.createReadStream({
        gt: {clusterId: clusterId},
        lt: lt,
        limit: limit,
        reverse: true
      });
      stream.on('data', (data) => {
        if (!first) res.write(",");
        res.write(JSON.stringify({
          balance: data.key.balance,
          address: data.key.address
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
  };

  private async addressBalanceChangesToClusterBalanceChanges(addressToDelta: Map<string, number>): Promise<Map<number, number>> {
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
    let result = {};
    clusterBalanceChanges.forEach((delta: number, clusterId: number) => {
      result[clusterId] = delta;
    });
    res.send(result);
  }  


  largestClusters = async (req:Request, res:Response) => {
    res.contentType('application/json');

    let limit = req.query.limit !== undefined ? Number(req.query.limit) : 100;
    if (!Number.isInteger(limit) || limit <= 0 || limit > 1000) {
      res.sendStatus(400);
      return;
    }


    let afterBalance: number;
    let afterCluster: number;
    if (req.query.after !== undefined) {
      let afterComponents = req.query.after.split('-');
      if (afterComponents.length > 2) {
        res.sendStatus(400);
        return;
      }
      afterBalance = Number(afterComponents[0]);
      if (!Number.isInteger(afterBalance) || afterBalance < 0) {
        res.sendStatus(400);
        return;
      }
      if (afterComponents.length === 2) {
        afterCluster = Number(afterComponents[1]);
        if (!Number.isInteger(afterCluster) || afterCluster < 0) {
          res.sendStatus(400);
          return;
        }
      }
    }

    let lt;
    if (afterBalance) {
      lt = {balance: afterBalance, clusterId: afterCluster};
    }

    res.write('[');
    let first = true;
    let stream = this.balanceToClusterTable.createReadStream({reverse: true, lt: lt, limit: limit})
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