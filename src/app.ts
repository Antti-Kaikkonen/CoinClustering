import { AbstractBatch } from 'abstract-leveldown';
import RpcClient from 'bitcoind-rpc';
import EncodingDown from 'encoding-down';
import express from 'express';
import http from 'http';
import LevelDOWN from 'leveldown';
import { Readable, Transform, Writable } from 'stream';
import { ClusterController } from './app/controllers/cluster-controller';
import { BlockWithTransactions } from './app/models/block';
import { Transaction } from './app/models/transaction';
import { AddressEncodingService } from './app/services/address-encoding-service';
import { BinaryDB } from './app/services/binary-db';
import { BlockImportService } from './app/services/block-import-service';
import { BlockService } from './app/services/block-service';
import { ClusterAddressService } from './app/services/cluster-address-service';
import { ClusterBalanceService } from './app/services/cluster-balance-service';
import { OutputCacheTable } from './app/tables/output-cache-table';
import { JSONtoAmount } from './app/utils/utils';



//export NODE_OPTIONS="--max_old_space_size=8192"

//export NODE_OPTIONS="--max_old_space_size=16384"

let cwd = process.cwd();
const config: any = require(cwd+'/config');

let addressEncodingService = new AddressEncodingService(config.pubkeyhash, config.scripthash, config.segwitprefix);


var rpc = new RpcClient(config);
let leveldown = LevelDOWN(cwd+'/db');


let db = new BinaryDB(EncodingDown<Buffer, Buffer>(leveldown, {keyEncoding: 'binary', valueEncoding: 'binary'}), {
  writeBufferSize: 8 * 1024 * 1024,
  cacheSize: 1000 * 1024 * 1024,
  compression: true
});

let clusterBalanceService = new ClusterBalanceService(db);

let clusterAddressService = new ClusterAddressService(db, addressEncodingService);

let blockService = new BlockService(db, rpc);

let clusterController = new ClusterController(db, addressEncodingService);

let blockImportService = new BlockImportService(db, clusterAddressService, clusterBalanceService, blockService, addressEncodingService);

let outputCacheTable = new OutputCacheTable(db, addressEncodingService);

const app = express();
app.get("/hello", clusterController.clusterCurrentBalances);
app.get("/hello2", clusterController.clusterTransactions);
app.get('/cluster_addresses/:id', clusterController.clusterAddresses);
app.get('/largest_clusters', clusterController.clustersByBalance);
app.listen(config.listen_port);

async function getBlockByHash(hash: string) {
  return new Promise<any>((resolve, reject) => {
    rpc.getBlock(hash, (error, ret) => {
      if (error) reject(error)
      else if (ret.error) reject(ret.error.message)
      else resolve(ret.result);
    });  
  });
}


//Each batch is executed concurrently in bitcoin core so making this value too large can lower performance. Too low value will increase overhead and exchaust rpcworkqueue
//Recommended to set rpcworkqueue=1024 and rpcthreads=64 in bitcoin.conf.
let rpc_batch_size = 30;


async function getTransactionsHelper(txids: string[]): Promise<Transaction[]> {
  let batchCall = () => {
    txids.forEach(txid => rpc.getRawTransaction(txid, 1));
  }
  return new Promise<Transaction[]>((resolve, reject) => {
    rpc.batch(batchCall, (err, rawtxs) => {
      if (err) reject(err)
      else if (rawtxs.length > 0 && rawtxs[0].error) reject(rawtxs[0].error.message)
      else resolve(rawtxs.map(rawtx => rawtx.result));
    });
  });  
}

async function getTransactions(txids: string[]): Promise<Transaction[]> {
  let res: Transaction[] = [];
  let from = 0;
  let promises = [];
  while (from < txids.length) {
    promises.push(getTransactionsHelper(txids.slice(from, from+rpc_batch_size)));//To avoid HTTP 413 error

    if (promises.length > 100) {
      let batches = await Promise.all(promises);
      batches.forEach(txs => txs.forEach(tx =>res.push(tx)));
      promises = [];
    }
    from+=rpc_batch_size;
  }
  let batches = await Promise.all(promises);
  batches.forEach(rawtxs => rawtxs.forEach(rawtx =>res.push(rawtx)));
  return res;
}

async function restblock(hash: string): Promise<BlockWithTransactions> {
  return new Promise<BlockWithTransactions>((resolve, reject) => {
    //console.log("http://"+config.host+":"+config.port+"/rest/block/"+hash+".json");
    http.get("http://"+config.host+":"+config.port+"/rest/block/"+hash+".json", (resp: http.IncomingMessage) => {

      let data = '';
      resp.on('data', (chunk) => {
        data += chunk;
      });
  
      resp.on('end', () => {
        resolve(JSON.parse(data));
      });
    });
  });
}

class InputFetcher extends Transform {
  
  constructor() {
    super({
      objectMode: true,
      //highWaterMark: 64,
      highWaterMark: 2,
      transform: (block: BlockWithTransactions, encoding, callback) => {
        let input_txids: Set<string> = new Set();
        let promises = [];
        let pendingOutputs = 0;
        block.tx.forEach((tx, n) => {
          tx.vin.forEach(vin => {
            if (vin.coinbase) return;
            if (vin.value === undefined) {
              pendingOutputs++;
              let promise = outputCacheTable.get({txid:vin.txid, n:vin.vout});
              let promise2 = new Promise((resolve, reject) => {
                promise.then((cachedOutput: {addresses: string[], valueSat: number}) => {
                  vin.value = cachedOutput.valueSat/1e8;
                  if (cachedOutput.addresses && cachedOutput.addresses.length === 1) vin.address = cachedOutput.addresses[0];
                  resolve();
                }, (err) => {
                  if (input_txids.has(vin.txid)) resolve();
                  input_txids.add(vin.txid);
                  resolve();
                });
              });
              promises.push(promise2);
            }
          });
        });
        Promise.all(promises).then(() => {
          let result: {
            block: BlockWithTransactions, 
            inputTxsPromise?: Promise<Transaction[]>
          } = {
            block: block
          }
          if (input_txids.size > 0) {
            console.log("attaching inputs...", input_txids.size);
            result.inputTxsPromise = getTransactions(Array.from(input_txids));
          }
          this.push(result);
          callback();
        });
      }
    });
  }
}

class InputAttacher extends Transform {
  
  constructor() {
    super({
      objectMode: true,
      //highWaterMark: 64,
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
          let promise = new Promise<BlockWithTransactions>((resolve, reject) => {
            rpc.getBlockHash(this.currentHeight, (err, res) => {
              let hash: string = res.result;
              restblock(hash).then((block: BlockWithTransactions) => {
                let cacheOps: AbstractBatch<Buffer, Buffer>[] = [];
                block.tx.forEach(tx => {
                  tx.vout.forEach(vout => {
                    try {
                      cacheOps.push(
                        outputCacheTable.putOperation({txid: tx.txid, n: vout.n}, {valueSat: JSONtoAmount(vout.value), addresses: vout.scriptPubKey.addresses})
                      );
                    } catch(err) {
                      console.log("ERR:", vout.value, JSONtoAmount(vout.value), vout.n);
                    }
                  });
                });
                db.batchBinary(cacheOps).then(() => {
                  resolve(block);
                })
              });
            });
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


async function getRpcHeight(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    rpc.getBlockCount((err, res) => {
      let height = res.result;
      resolve(height);
    });
  });
}

async function deleteBlockInputs(block: BlockWithTransactions) {
  let delOps = [];
  block.tx.forEach(tx => {
    tx.vin.forEach(vin => {
      if (vin.coinbase) return;
      delOps.push(outputCacheTable.delOperation({txid: vin.txid, n: vin.vout}));
    });
  });
  db.batchBinary(delOps);
}


doProcessing();

async function doProcessing() {
  let height = await getRpcHeight();
  console.log("rpc height", height);
  let lastMergedHeight: number = await blockImportService.getLastMergedHeight();
  let lastSavedTxHeight: number = await blockImportService.getLastSavedTxHeight();
  let blockWriter: Writable;
  let startHeight: number;
  let stayBehind = 100;
  let toHeight: number;
  if (lastMergedHeight < height-stayBehind) {
    startHeight = lastMergedHeight > -1 ? lastMergedHeight + 1 : 1;
    toHeight = height-stayBehind;
    console.log("merging between blocks", startHeight, "and", toHeight);
    blockWriter = new Writable({
      objectMode: true,
      //highWaterMark: 256,
      write: async (block: BlockWithTransactions, encoding, callback) => {
        await blockImportService.blockMerging(block);
        if (lastSavedTxHeight === -1) deleteBlockInputs(block);
        callback(null);
      }
    });
  } else if (lastSavedTxHeight < height-stayBehind) {
    startHeight = lastSavedTxHeight > -1 ? lastSavedTxHeight + 1 : 1;
    toHeight = lastMergedHeight;
    console.log("saving transactions between blocks", startHeight, "and", toHeight);
    blockWriter = new Writable({
      objectMode: true,
      //highWaterMark: 256,
      write: async (block: BlockWithTransactions, encoding, callback) => {
        await blockImportService.saveBlockTransactions(block);
        deleteBlockInputs(block);
        callback(null);
      }
    });
  } else {
    setTimeout(doProcessing, 10000);
    return;
  }
  let blockReader = new BlockByHeightReader(startHeight, toHeight);
  let blockAttacher = new BlockAttacher();
  let inputFetcher = new InputFetcher();
  let inputAttacher = new InputAttacher();

  let interval = setInterval(()=>{
    console.log("blockReader",blockReader.readableLength);
    console.log("blockAttacher", blockAttacher.readableLength, blockAttacher.writableLength)
    //console.log("txAttacher", txAttacher.readableLength, txAttacher.writableLength);
    console.log("inputFetcher", inputFetcher.readableLength, inputFetcher.writableLength);
    console.log("inputAttacher", inputAttacher.readableLength, inputAttacher.writableLength);
    console.log("blockWriter", blockWriter.writableLength);
  }, 5000);

  blockReader.pipe(blockAttacher).pipe(inputFetcher).pipe(inputAttacher).pipe(blockWriter);
  blockReader.on('end', () => {
  });
  blockWriter.on('finish', () => {
    clearInterval(interval);
    setTimeout(doProcessing, 0);
  });
}