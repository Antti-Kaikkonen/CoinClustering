import { AbstractBatch } from "abstract-leveldown";
import { injectable } from "inversify";
import { Readable, Transform } from "stream";
import RestApi from '../misc/rest-api';
import RpcApi from '../misc/rpc-api';
import { JSONtoAmount } from "../misc/utils";
import { BlockWithTransactions } from "../models/block";
import { Transaction } from "../models/transaction";
import { OutputCacheTable } from "../tables/output-cache-table";
import { AddressEncodingService } from "./address-encoding-service";
import { BinaryDB } from "./binary-db";

let _outputCacheTable: OutputCacheTable;
let _restApi: RestApi;
let _rpcApi: RpcApi;
let _db: BinaryDB;

@injectable()
export class BlockchainReader {
  constructor(restApi: RestApi, rpcApi: RpcApi, addressEncodingService: AddressEncodingService, db: BinaryDB) {
    _restApi = restApi;
    _rpcApi = rpcApi;
    _db = db;
    _outputCacheTable = new OutputCacheTable(db, addressEncodingService);
  }

  createReadStream(startHeight: number, toHeight: number): Transform {
    let blockReader = new BlockByHeightReader(startHeight, toHeight);
    let blockAttacher = new BlockAttacher();
    let inputFetcher = new InputFetcher();
    let inputAttacher = new InputAttacher();
    return blockReader.pipe(blockAttacher).pipe(inputFetcher).pipe(inputAttacher);
  }
}

class InputFetcher extends Transform {
  constructor() {
    super({
      objectMode: true,
      highWaterMark: 2,
      transform: async (block: BlockWithTransactions, encoding, callback) => {
        let input_txids: Set<string> = new Set();
        let promises = [];
        block.tx.forEach((tx, n) => {
          tx.vin.forEach(vin => {
            if (vin.coinbase) return;
            if (vin.value === undefined) {
              let promise = new Promise<void>(async (resolve, reject) => {
                try {
                  let cachedOutput = await _outputCacheTable.get({txid:vin.txid, n:vin.vout});
                  vin.value = cachedOutput.valueSat/1e8;
                  if (cachedOutput.addresses && cachedOutput.addresses.length === 1) vin.address = cachedOutput.addresses[0];
                  resolve();
                } catch(err) {
                  if (!input_txids.has(vin.txid)) input_txids.add(vin.txid);
                  resolve();
                }
              });
              promises.push(promise);
            }
          });
        });
        await Promise.all(promises);
        let result: {
          block: BlockWithTransactions, 
          inputTxsPromise?: Promise<Transaction[]>
        } = {
          block: block
        }
        if (input_txids.size > 0) {
          console.log("attaching inputs...", input_txids.size);
          result.inputTxsPromise = _rpcApi.getTransactions(Array.from(input_txids));
        }
        this.push(result);
        callback();
      }
    });
  }
}

class InputAttacher extends Transform {
  constructor() {
    super({
      objectMode: true,
      highWaterMark: 2,
      transform: async (blockAndInputs: {block: BlockWithTransactions, inputTxsPromise?: Promise<Transaction[]>} , encoding, callback) => {
        let block = blockAndInputs.block;
        
        let inputTxsPromise = blockAndInputs.inputTxsPromise;
        if (inputTxsPromise !== undefined) {
          let inputTxs: Transaction[] = await inputTxsPromise;
          console.log("inputAttacher. got ", inputTxs.length, "inputs");
          let txidToInputTx: Map<string, Transaction> = new Map();
          inputTxs.forEach(tx => txidToInputTx.set(tx.txid, tx));

          block.tx.forEach(tx => {
            tx.vin.forEach(vin => {
              if (vin.coinbase) return;
              if (vin.value === undefined) {
                let inputTx = txidToInputTx.get(vin.txid);
                vin.value = inputTx.vout[vin.vout].value;
                let pubkey = inputTx.vout[vin.vout].scriptPubKey;
                if (pubkey.addresses && pubkey.addresses.length === 1) 
                  vin.address = pubkey.addresses[0];
              }  
            });
          });
        }
        this.push(block);
        callback();
      }
    });
  }
}

class BlockByHeightReader extends Readable {
  currentHeight: number;
  constructor(startHeight: number, stopHeight: number) {
    super({
      objectMode: true,
      highWaterMark: 4,
      read: async (size) => {
        if (this.currentHeight !== undefined && this.currentHeight > stopHeight) 
          this.push(null);
        else while (true) {
          let promise = new Promise<BlockWithTransactions>(async (resolve, reject) => {
            let hash = await _rpcApi.getRpcBlockHash(this.currentHeight);
            let block = await _restApi.restblock(hash);
            let cacheOps: AbstractBatch<Buffer, Buffer>[] = [];
            block.tx.forEach(tx => {
              tx.vout.forEach(vout => {
                cacheOps.push(
                  _outputCacheTable.putOperation({txid: tx.txid, n: vout.n}, {valueSat: JSONtoAmount(vout.value), addresses: vout.scriptPubKey.addresses})
                );
              });
            });
            await _db.batchBinary(cacheOps);
            resolve(block);
          });
          this.currentHeight++;
          let shouldBreak = this.push(promise);
          break;//if (shouldBreak) break;//async push fixed in node 10 https://github.com/nodejs/node/pull/17979
        }
        
      }
    });
    this.currentHeight = startHeight;
  }
};

class BlockAttacher extends Transform {
  constructor() {
    super({
      objectMode: true,
      highWaterMark: 2,
      transform: async (blockPromise:  Promise<BlockWithTransactions> , encoding, callback) => {
        let block = await blockPromise;
        this.push(block);
        callback();
      }
    });
  }
};