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

const config: RpcClientOptions = require('../config');

var rpc = new RpcClient(config);
let rocksdb = RocksDB('./db');
let db = levelup(encoding(rocksdb));

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

let firstBlock: boolean = true;
const blockWriter = new Writable({
  objectMode: true,
  highWaterMark: 4,
  write: async (block, encoding, callback) => {
    await blockImportService.saveBlock(block);
    firstBlock = false;
    callback(null);
  }
});

class BlockReader extends Readable {
  currentHash: string;
  constructor(hash: string) {
    super({
      objectMode: true,
      highWaterMark: 32,
      read: async (size) => {
        while (true) {
          let block = await getBlockByHash(this.currentHash);
          let rawtxs = await getRawTransactions(block.tx);
          let txs = await decodeRawTransactions(rawtxs);
          block.tx = txs;
          this.currentHash = block.nextblockhash;
          let shouldBreak = this.push(block);
          break;//if (shouldBreak) break;//async push fixed in node 10 https://github.com/nodejs/node/pull/17979
        }
        
      }
    });
    this.currentHash = hash;
  }
};  



process();

async function process() {
  let tipInfo = await blockService.getTipInfo();
  console.log("tipInfo", tipInfo);
  if (tipInfo !== undefined && tipInfo.reorgDepth > 0) {
    //TODO: process reorg
    await process();
    return;
  }
  let hash = await blockService.getRpcBlockHash(tipInfo !== undefined ? tipInfo.lastSavedHeight+1 : 1);
  let blockReader = new BlockReader(hash);
  blockReader.pipe(blockWriter);
  blockWriter.on('close', () => {
    setTimeout(process, 1000);
  });
}