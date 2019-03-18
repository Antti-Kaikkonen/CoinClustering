import { Request, Response } from 'express';
import { injectable } from 'inversify';
import { AddressTransaction } from '../models/address-transaction';
import { AddressService } from '../services/address-service';
import { AddressTransactionTable } from '../tables/address-transaction-table';

@injectable()
export class AddressController {

  constructor(private addressService: AddressService,
  private addressTransactionTable: AddressTransactionTable) {
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


}  