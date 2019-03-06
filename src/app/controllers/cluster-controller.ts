import { Request, Response } from 'express';
import RpcApi from '../misc/rpc-api';
import { txAddressBalanceChanges } from '../misc/utils';
import { ClusterTransaction } from '../models/cluster-transaction';
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { ClusterAddressService } from '../services/cluster-address-service';
import { ClusterTransactionService } from '../services/cluster-transaction-service';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterAddressTable } from '../tables/cluster-address-table';
import { ClusterMergedToTable } from '../tables/cluster-merged-to-table';
import { ClusterTransactionTable } from '../tables/cluster-transaction-table';
import { OutputCacheTable } from '../tables/output-cache-table';


export class ClusterController {

  private clusterTransactionService: ClusterTransactionService;
  private clusterAddressService: ClusterAddressService;
  private balanceToClusterTable: BalanceToClusterTable;
  private clusterMergedToTable: ClusterMergedToTable;
  private clusterTransactionTable: ClusterTransactionTable;
  private clusterAddressTable: ClusterAddressTable;
  private outputCacheTable: OutputCacheTable;

  constructor(db: BinaryDB, addressEncodingService: AddressEncodingService, private rpcApi: RpcApi) {
    this.clusterTransactionService = new ClusterTransactionService(db);
    this.clusterAddressService = new ClusterAddressService(db, addressEncodingService);
    this.balanceToClusterTable = new BalanceToClusterTable(db);
    this.clusterMergedToTable = new ClusterMergedToTable(db);
    this.clusterTransactionTable = new ClusterTransactionTable(db);
    this.clusterAddressTable = new ClusterAddressTable(db, addressEncodingService);
    this.outputCacheTable = new OutputCacheTable(db, addressEncodingService);
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
      let queryPos = req.url.indexOf("?");
      if (queryPos >= 0) newPath += req.url.substr(queryPos);
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

  private decodeHeightAndNToken(clusterId: number, str?: string): {clusterId: number, height: number, n?: number} {
    if (str === undefined) return;
    let components = str.split('-');
    if (components.length > 2) throw new Error("Too many components");
    let height = Number(components[0]);
    if (!Number.isInteger(height) || height < 0) throw new Error("Invalid height");
    let n;
    if (components.length === 2) {
      n = Number(components[1]);
      if (!Number.isInteger(n) || n < 0) throw new Error("Invalid n");
    }
    return {clusterId: clusterId, height: height, n: n};

  }


  clusterTransactions = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);

    let limit = req.query.limit !== undefined ? Number(req.query.limit) : 100;
    if (!Number.isInteger(limit) || limit <= 0 || limit > 1000) {
      res.sendStatus(400);
      return;
    }

    let options;
    try {
      options = {
        reverse: req.query.reverse === "true",
        lt: this.decodeHeightAndNToken(clusterId, req.query.lt), 
        lte: this.decodeHeightAndNToken(clusterId, req.query.lte), 
        gt: this.decodeHeightAndNToken(clusterId, req.query.gt), 
        gte: this.decodeHeightAndNToken(clusterId, req.query.gte),
        limit: limit,
        fillCache: true
      };
    } catch(err) {
      res.sendStatus(400);
      return;
    }
    if (!options.lt && !options.lte) options.lt = {clusterId: clusterId+1};
    if (!options.gt && !options.gte) options.gt = {clusterId: clusterId};

    

    let redirectToCluster = await this.redirectToCluster(clusterId);
    if (redirectToCluster !== undefined) {
      let newPath = req.baseUrl+req.route.path.replace(':id', redirectToCluster);
      let queryPos = req.url.indexOf("?");
      if (queryPos >= 0) newPath += req.url.substr(queryPos);
      res.redirect(301, newPath);
    } else {
      //res.contentType('application/json');
      //res.write('[');
      //let stream = this.clusterTransactionTable.createReadStream(options);

      let clusterTransactions: ClusterTransaction[] = [];
      try {
        await new Promise((resolve, reject) => {
          let stream = this.clusterTransactionTable.createReadStream(options);
          stream.on('data', (data) => {
            clusterTransactions.push(new ClusterTransaction(
              data.value.txid,
              data.key.height,
              data.key.n
            ));
          }).on('finish', () => {
            resolve();
          });
          req.on('close', () => {
            console.log("cancelled by user. destroying");
            stream['destroy']();
            console.log("destroyed");
            reject();
          });
        });
      } catch(err) {
        return;
      }

      if (req.query['include-delta'] !== "true") {
        res.json(clusterTransactions);
        return;
      }

      let rpcTransactions = await this.rpcApi.getTransactions(clusterTransactions.map(tx => tx.txid));

      let attachInputsPromises = [];
      rpcTransactions.forEach(tx => {
        let attachInputsPromise = new Promise((resolve, reject) => {
          let inputsToAttach = tx.vin.length;
          if (inputsToAttach === 0) resolve();
          tx.vin.forEach(vin => {
            if (!vin.coinbase && !vin.address) {
              this.outputCacheTable.get({txid: vin.txid, n: vin.vout}).then((value) => {
                if (value.addresses.length === 1) {
                  vin.address = value.addresses[0];
                  vin.value = value.valueSat/1e8;
                }
                inputsToAttach--;
                if (inputsToAttach === 0) resolve();
              })
            } else {
              inputsToAttach--;
              if (inputsToAttach === 0) resolve();
            }
          });
        }); 
        attachInputsPromises.push(attachInputsPromise);
      });
      await Promise.all(attachInputsPromises);
      let clusterBalanceChangesPromises = [];
      rpcTransactions.forEach(tx => {
        let balanceChanges: Map<string, number> = txAddressBalanceChanges(tx);
        clusterBalanceChangesPromises.push(this.clusterAddressService.addressBalanceChangesToClusterBalanceChanges(balanceChanges));
      });  

      let clusterBalanceChanges = await Promise.all(clusterBalanceChangesPromises);
      clusterBalanceChanges.forEach((txClusterBalanceChanges, index) => {
          let delta = txClusterBalanceChanges.get(clusterId);
          let clusterTransaction = clusterTransactions[index];
          clusterTransaction['delta'] = delta;
      });
      res.json(clusterTransactions);
    }
  };

  private decodeBalanceAndAddressToken(clusterId: number, str?: string): {clusterId: number, balance: number, address?: string} {
    if (str === undefined) return;
    let components = str.split('-');
    if (components.length > 2) throw new Error("Too many components");
    let balance = Number(components[0]);
    if (!Number.isInteger(balance) || balance < 0) throw new Error("Invalid balance");
    let address;
    if (components.length === 2) {
      address = components[1];
    }
    return {clusterId: clusterId, balance: balance, address: address};

  }

  clusterAddresses = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);

    let limit = req.query.limit !== undefined ? Number(req.query.limit) : 100;
    if (!Number.isInteger(limit) || limit <= 0 || limit > 1000) {
      res.sendStatus(400);
      return;
    }

    let options;
    try {
      options = {
        reverse: req.query.reverse === "true",
        lt: this.decodeBalanceAndAddressToken(clusterId, req.query.lt), 
        lte: this.decodeBalanceAndAddressToken(clusterId, req.query.lte), 
        gt: this.decodeBalanceAndAddressToken(clusterId, req.query.gt), 
        gte: this.decodeBalanceAndAddressToken(clusterId, req.query.gte),
        limit: limit,
        fillCache: true
      };
    } catch(err) {
      res.sendStatus(400);
      return;
    }
    if (!options.lt && !options.lte) options.lt = {clusterId: clusterId+1};
    if (!options.gt && !options.gte) options.gt = {clusterId: clusterId};

    let redirectToCluster = await this.redirectToCluster(clusterId);
    if (redirectToCluster !== undefined) {
      let newPath = req.baseUrl+req.route.path.replace(':id', redirectToCluster);
      let queryPos = req.url.indexOf("?");
      if (queryPos >= 0) newPath += req.url.substr(queryPos);
      res.redirect(301, newPath);
    } else {

      res.contentType('application/json');
      res.write('[');
      let first = true;
    
      let stream = this.clusterAddressTable.createReadStream(options);
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

  private decodeBalanceClusterIdToken(str?: string): {balance: number, clusterId?: number} {
    if (str === undefined) return;
    let components = str.split('-');
    if (components.length > 2) throw new Error("Too many components");
    let balance = Number(components[0]);
    if (!Number.isInteger(balance) || balance < 0) throw new Error("Invalid balance");
    let clusterId;
    if (components.length === 2) {
      clusterId = Number(components[1]);
      if (!Number.isInteger(clusterId) || clusterId < 0) throw new Error("Invalid clusterId");
    }
    return {balance: balance, clusterId: clusterId};

  }

  largestClusters = async (req:Request, res:Response) => {
    res.contentType('application/json');

    let limit = req.query.limit !== undefined ? Number(req.query.limit) : 100;
    if (!Number.isInteger(limit) || limit <= 0 || limit > 1000) {
      res.sendStatus(400);
      return;
    }

    let options;
    try {
      options = {
        reverse: req.query.reverse === "true",
        lt: this.decodeBalanceClusterIdToken(req.query.lt), 
        lte: this.decodeBalanceClusterIdToken(req.query.lte), 
        gt: this.decodeBalanceClusterIdToken(req.query.gt), 
        gte: this.decodeBalanceClusterIdToken(req.query.gte),
        limit: limit,
        fillCache: true
      };
    } catch(err) {
      res.sendStatus(400);
      return;
    }

    res.write('[');
    let first = true;
    let stream = this.balanceToClusterTable.createReadStream(options)
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