import RpcClient, { RpcClientOptions } from 'bitcoind-rpc';
import encoding from 'encoding-down';
import express from 'express';
import levelup from 'levelup';
import RocksDB from 'rocksdb';
import { Readable, Writable } from 'stream';
import { ClusterController } from './app/controllers/cluster-controller';
import { BlockImportService } from './app/services/block-import-service';
import { BlockService } from './app/services/block-service';
import { ClusterAddressService } from './app/services/cluster-address-service';
import { ClusterBalanceService } from './app/services/cluster-balance-service';

let cwd = process.cwd();
let args = process.argv.slice(2);
const config: RpcClientOptions = require(cwd+'/config');

var rpc = new RpcClient(config);
let rocksdb = RocksDB(cwd+'/db');
let db = levelup(encoding(rocksdb), {
  writeBufferSize: 8 * 1024 * 1024,
  cacheSize: 1024 * 1024 * 1024
});

let clusterBalanceService = new ClusterBalanceService(db);

let clusterAddressService = new ClusterAddressService(db);

let blockService = new BlockService(db, rpc);

let clusterController = new ClusterController(clusterBalanceService, clusterAddressService);

let blockImportService = new BlockImportService(db, clusterAddressService, clusterBalanceService, blockService);

const app = express();
app.get("/hello", clusterController.clusterCurrentBalances);
app.get("/hello2", clusterController.clusterTransactions);
app.get("/hello3", clusterController.clusterAddresses);
app.get('/cluster_addresses/:id', clusterController.clusterAddresses);
app.listen(3006);

console.log("cwd", cwd);
console.log("args", args);

async function getBlockByHash(hash: string) {
  return new Promise<any>((resolve, reject) => {
    rpc.getBlock(hash, (error, ret) => {
      if (error) reject(error)
      else resolve(ret.result);
    });  
  });
}

async function decodeRawTransactions(rawtxs: any[]) {
  let batchCall = () => {
    rawtxs.forEach(rawtx => rpc.decodeRawTransaction(rawtx));
  }
  return new Promise<any>((resolve, reject) => {
    rpc.batch(batchCall, (err, txs) => {
      if (err) reject(err)
      else resolve(txs.map(tx => tx.result));
    });
  });
}

async function getRawTransactions(txids: string[]) {
  let batchCall = () => {
    txids.forEach(txid => rpc.getRawTransaction(txid));
  }
  return new Promise<any>((resolve, reject) => {
    rpc.batch(batchCall, (err, rawtxs) => {
      if (err) reject(err)
      else resolve(rawtxs.map(rawtx => rawtx.result));
    });
  });  
}

class BlockReader extends Readable {
  currentHash: string;
  currentHeight: number;
  constructor(hash: string, stopHeight: number) {
    super({
      objectMode: true,
      highWaterMark: 32,
      read: async (size) => {
        if (this.currentHeight !== undefined && this.currentHeight > stopHeight) 
          this.push(null);
        else while (true) {
          let block = await getBlockByHash(this.currentHash);
          let rawtxs = await getRawTransactions(block.tx);
          let txs = await decodeRawTransactions(rawtxs);
          block.tx = txs;
          this.currentHash = block.nextblockhash;
          this.currentHeight = block.height+1;
          let shouldBreak = this.push(block);
          break;//if (shouldBreak) break;//async push fixed in node 10 https://github.com/nodejs/node/pull/17979
        }
        
      }
    });
    this.currentHash = hash;
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


doProcessing();

async function doProcessing() {
  let height = await getRpcHeight();
  console.log("rpc height", height);
  /*let tipInfo = await blockService.getTipInfo();
  console.log("tipInfo", tipInfo);
  if (tipInfo !== undefined && tipInfo.reorgDepth > 0) {
    //TODO: process reorg
    await doProcessing();
    return;
  }
  let hash = await blockService.getRpcBlockHash(tipInfo !== undefined ? tipInfo.lastSavedHeight+1 : 1);*/
  let lastMergedHeight: number = await blockImportService.getLastMergedHeight();
  let lastSavedTxHeight: number = await blockImportService.getLastSavedTxHeight();
  let blockWriter: Writable;
  let startHeight: number;
  let stayBehind = 100;
  let toHeight: number;
  if (lastMergedHeight < height-stayBehind) {
    console.log("merging");
    startHeight = lastMergedHeight > -1 ? lastMergedHeight + 1 : 1;
    toHeight = height-stayBehind;
    blockWriter = new Writable({
      objectMode: true,
      highWaterMark: 4,
      write: async (block, encoding, callback) => {
        await blockImportService.blockMerging(block);
        callback(null);
      }
    });
  } else if (lastSavedTxHeight < height-stayBehind) {
    console.log("saving transactions");
    startHeight = lastSavedTxHeight > -1 ? lastSavedTxHeight + 1 : 1;
    toHeight = lastMergedHeight;
    blockWriter = new Writable({
      objectMode: true,
      highWaterMark: 4,
      write: async (block, encoding, callback) => {
        await blockImportService.saveBlockTransactions(block);
        callback(null);
      }
    });
  } else {
    setTimeout(doProcessing, 10000);
    return;
  }

  let startHash: string = await blockService.getRpcBlockHash(startHeight);
  let blockReader = new BlockReader(startHash, toHeight);
  blockReader.pipe(blockWriter);
  blockReader.on('end', () => {
  });
  blockWriter.on('finish', () => {
    setTimeout(doProcessing, 0);
  });
}