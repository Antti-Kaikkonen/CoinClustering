import { Request, Response } from 'express';
import { injectable } from 'inversify';
import { Transform, Writable } from 'stream';
import RestApi from '../misc/rest-api';
import RpcApi from '../misc/rpc-api';
import { ClusterTransaction } from '../models/cluster-transaction';
import { BlockTimeService } from '../services/block-time-service';
import { ClusterAddressService } from '../services/cluster-address-service';
import { ClusterTransactionService } from '../services/cluster-transaction-service';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';
import { ClusterAddressTable } from '../tables/cluster-address-table';
import { ClusterMergedToTable } from '../tables/cluster-merged-to-table';
import { ClusterTransactionTable } from '../tables/cluster-transaction-table';

@injectable()
export class ClusterController {

  constructor(
    private clusterTransactionService: ClusterTransactionService,
    private clusterAddressService: ClusterAddressService,
    private balanceToClusterTable: BalanceToClusterTable,
    private clusterMergedToTable: ClusterMergedToTable,
    private clusterTransactionTable: ClusterTransactionTable,
    private clusterAddressTable: ClusterAddressTable,
    private blockTimeService: BlockTimeService,
    private rpcApi: RpcApi,
    private restApi: RestApi) {
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
      let addressCountPromise = this.clusterAddressService.getAddressCountDefaultUndefined(clusterId);
      let balancePromise = this.clusterTransactionService.getClusterBalanceDefaultZero(clusterId);
      let transactionCountPromise = this.clusterTransactionService.getClusterTransactionCountDefaultZero(clusterId);
      let firstTransactionPromise = this.clusterTransactionService.getFirstClusterTransaction(clusterId);
      let lastTransactionPromise = this.clusterTransactionService.getLastClusterTransaction(clusterId);
      let addressCount = await addressCountPromise;
      if (addressCount === undefined) {
        res.sendStatus(404);
      } else {
        res.send({
          balance: await balancePromise,
          firstTransaction: await firstTransactionPromise,
          lastTransaction: await lastTransactionPromise,
          addressCount: await addressCountPromise,
          transactionCount: await transactionCountPromise
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
      res.contentType('application/json');
      res.write('[');
      let first = true;
      let stream = this.clusterTransactionTable.createReadStream(options);
      stream.on('data', (data) => {
        if (!first) res.write(",");
        res.write(JSON.stringify(new ClusterTransaction(
          data.value.txid,
          data.key.height,
          data.key.n,
          data.value.balanceChange
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
  }

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

  clusters = async (req:Request, res:Response) => {
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

  candleSticks = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);
    let redirectToCluster = await this.redirectToCluster(clusterId);
    if (redirectToCluster !== undefined) {
      let newPath = req.baseUrl+req.route.path.replace(':id', redirectToCluster);
      let queryPos = req.url.indexOf("?");
      if (queryPos >= 0) newPath += req.url.substr(queryPos);
      res.redirect(301, newPath);
    } else {
      res.contentType('application/json');
      let candleSticks: Map<number, {open: number, close: number, low: number, high: number, date: number}> = new Map();
      let currentBalance = 0;
      res.write('[');
      let first = true;
      let previousCandle: {open: number, close: number, low: number, high: number, date: number};
      let toTxAndTimePromise = new Transform({
        objectMode: true,
        transform: async (data: {key: {height: number, n: number}, value: {txid: string, balanceChange: number}} , encoding, callback) => {
          let tx: ClusterTransaction = new ClusterTransaction(
            data.value.txid,
            data.key.height,
            data.key.n,
            data.value.balanceChange
          );
          toTxAndTimePromise.push({tx: tx, timePromise: this.blockTimeService.getTime(tx.height)});
          callback();
        }
      });

      let writer = new Writable({
        objectMode: true,
        write: async (txAndTime: {tx: ClusterTransaction, timePromise: Promise<number>}, encoding, callback) => {
          let tx: ClusterTransaction = txAndTime.tx;
          currentBalance += tx.balanceChange;
          let time = await txAndTime.timePromise;
          let epochDate = Math.floor(time/(60*60*24));
          let candle = candleSticks.get(epochDate);
          if (candle === undefined) {
            candle = {open: currentBalance-tx.balanceChange, close: currentBalance, low: Math.min(currentBalance, currentBalance-tx.balanceChange), high: Math.max(currentBalance, currentBalance-tx.balanceChange), date: epochDate};
            candleSticks.set(epochDate, candle);
            if (previousCandle === undefined) {
              previousCandle = candle;
            } else if (previousCandle !== candle) {
              if (!first) res.write(",");
              first = false;
              res.write(JSON.stringify(previousCandle));
              previousCandle = candle;
            }
          } else {
            candle.close = currentBalance;
            if (currentBalance < candle.low) candle.low = currentBalance;
            if (currentBalance > candle.high) candle.high = currentBalance;
          }
          callback(null);
        }
      });
      
      let stream = this.clusterTransactionTable.createReadStream({
        lt: {clusterId: clusterId+1}, 
        gt: {clusterId: clusterId}
      });
      stream.pipe(toTxAndTimePromise).pipe(writer).on('finish', () => {
        if (previousCandle !== undefined) {
          if (!first) res.write(",");
          res.write(JSON.stringify(previousCandle));
        }  
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

}  