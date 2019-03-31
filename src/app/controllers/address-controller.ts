import { Request, Response } from 'express';
import { injectable } from 'inversify';
import { Transform, Writable } from 'stream';
import { AddressTransaction } from '../models/address-transaction';
import { AddressService } from '../services/address-service';
import { BlockTimeService } from '../services/block-time-service';
import { AddressTransactionTable } from '../tables/address-transaction-table';

@injectable()
export class AddressController {

  constructor(private addressService: AddressService,
  private addressTransactionTable: AddressTransactionTable,
  private blockTimeService: BlockTimeService) {
  }  

  clusterId = async (req:Request, res:Response) => {
    let address: string = req.params.address;
    try {
      let clusterId = await this.addressService.getAddressCluster(address);
      res.send(clusterId.toString());
    } catch(err) {
      res.sendStatus(404);
    }
  };

  private decodeHeightAndNToken(address: string, str?: string): {address: string, height: number, n?: number} {
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
    return {address: address, height: height, n: n};
  }

  addressTransactions = async (req:Request, res:Response) => {
    let address: string = req.params.address;
    let limit = req.query.limit !== undefined ? Number(req.query.limit) : 100;
    let options;
    try {
      options = {
        reverse: req.query.reverse === "true",
        lt: this.decodeHeightAndNToken(address, req.query.lt), 
        lte: this.decodeHeightAndNToken(address, req.query.lte), 
        gt: this.decodeHeightAndNToken(address, req.query.gt), 
        gte: this.decodeHeightAndNToken(address, req.query.gte),
        limit: limit,
        fillCache: true
      };
    } catch(err) {
      res.sendStatus(400);
      return;
    }
    if (!options.lt && !options.lte) options.lt = {address: address, height: 100000000};
    if (!options.gt && !options.gte) options.gt = {address: address};
    console.log("options", options);
    let stream = this.addressTransactionTable.createReadStream(options);
    res.contentType('application/json');
    res.write('[');
    let first = true;
    stream.on('data', (data) => {
      console.log("data", data);
      if (!first) res.write(",");

      res.write(JSON.stringify(new AddressTransaction(
        data.value.txid,
        data.key.height,
        data.key.n,
        data.value.balance
      )));
      first = false;
    }).on('finish', () => {
      res.write(']');
      res.end();
    });
    req.on('close', () => {
      stream['destroy']();
    });
  }  

  candleSticks = async (req:Request, res:Response) => {
    let address: string = req.params.address;

    res.contentType('application/json');
    let candleSticks: Map<number, {open: number, close: number, low: number, high: number, date: number}> = new Map();
    res.write('[');
    let first = true;
    let previousCandle: {open: number, close: number, low: number, high: number, date: number};

    let toTxAndTimePromise = new Transform({
      objectMode: true,
      transform: async (data: {key: {height: number, n: number}, value: {txid: string, balance: number}} , encoding, callback) => {
        let tx: AddressTransaction = new AddressTransaction(
          data.value.txid,
          data.key.height,
          data.key.n,
          data.value.balance
        );
        toTxAndTimePromise.push({tx: tx, timePromise: this.blockTimeService.getTime(tx.height)});
        callback();
      }
    });

    let currentBalance = 0;
    let previousBalance = 0;
    let writer = new Writable({
      objectMode: true,
      write: async (txAndTime: {tx: AddressTransaction, timePromise: Promise<number>}, encoding, callback) => {
        let tx: AddressTransaction = txAndTime.tx;
        currentBalance = tx.balance;
        let time = await txAndTime.timePromise;
        let epochDate = Math.floor(time/(60*60*24));
        let candle = candleSticks.get(epochDate);
        if (candle === undefined) {
          candle = {open: previousBalance, close: currentBalance, low: Math.min(currentBalance, previousBalance), high: Math.max(currentBalance, previousBalance), date: epochDate};
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
        previousBalance = currentBalance;
        callback(null);
      }
    });
    let stream = this.addressTransactionTable.createReadStream({
      lt: {address: address, height: 100000000}, 
      gt: {address: address}
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