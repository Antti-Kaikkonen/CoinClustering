import RpcClient from 'bitcoind-rpc';
import EncodingDown from 'encoding-down';
import express from 'express';
import LevelDOWN from 'leveldown';
import LRU, { Cache } from 'lru-cache';
import { Readable, Transform, Writable } from 'stream';
import { ClusterController } from './app/controllers/cluster-controller';
import { Block, BlockWithTransactions } from './app/models/block';
import { Transaction } from './app/models/transaction';
import { BinaryDB } from './app/services/binary-db';
import { BlockImportService } from './app/services/block-import-service';
import { BlockService } from './app/services/block-service';
import { ClusterAddressService } from './app/services/cluster-address-service';
import { ClusterBalanceService } from './app/services/cluster-balance-service';



//export NODE_OPTIONS="--max_old_space_size=4096 or 8192

let outputCache: Cache<string, {value: number, addresses: string[]}> = new LRU({max: 3000000});

let cwd = process.cwd();
let args = process.argv.slice(2);
const config: any = require(cwd+'/config');

/*RocksDB['repair'](cwd+'/db', (err) => {
  console.log("err",err);
  process.exit();
});
console.log("repaired");*/


var rpc = new RpcClient(config);
let leveldown = LevelDOWN(cwd+'/db');


let db = new BinaryDB(EncodingDown<Buffer, Buffer>(leveldown, {keyEncoding: 'binary', valueEncoding: 'binary'}), {
  writeBufferSize: 8 * 1024 * 1024,
  cacheSize: 256 * 1024 * 1024,
  compression: false
});

let clusterBalanceService = new ClusterBalanceService(db);

let clusterAddressService = new ClusterAddressService(db);

let blockService = new BlockService(db, rpc);

let clusterController = new ClusterController(clusterBalanceService, clusterAddressService);

let blockImportService = new BlockImportService(db, clusterAddressService, clusterBalanceService, blockService);

const app = express();
app.get("/hello", clusterController.clusterCurrentBalances);
app.get("/hello2", clusterController.clusterTransactions);
app.get('/cluster_addresses/:id', clusterController.clusterAddresses);
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

async function decodeRawTransactionsHelper(rawtxs: any[]): Promise<Transaction[]> {
  let batchCall = () => {
    rawtxs.forEach(rawtx => rpc.decodeRawTransaction(rawtx));
  }
  return new Promise<any>((resolve, reject) => {

    let txids = [];

    rpc.batch(batchCall, (err, txs) => {
      if (err) reject(err)
      else if (txs.length > 0 && txs[0].error) reject(txs[0].error.message)
      else resolve(txs.map(tx => tx.result)); 
    });
  });
}


//Each batch is executed concurrently in bitcoin core so making this value too large can lower performance. Too low value will increase overhead and exchaust rpcworkqueue
//Recommended to set rpcworkqueue=1024 and rpcthreads=64 in bitcoin.conf.
let rpc_batch_size = 10;

async function decodeRawTransactions(rawtxs: any[]): Promise<Transaction[]> {
  let res = [];
  let from = 0;
  let promises = [];
  while (from < rawtxs.length) {
    promises.push(decodeRawTransactionsHelper(rawtxs.slice(from, from+rpc_batch_size)));
    if (promises.length > 500) {
      let batches = await Promise.all(promises);
      batches.forEach(txs => txs.forEach(tx =>res.push(tx)));
      promises = [];
    }
    from+=rpc_batch_size;
  }
  let batches = await Promise.all(promises);
  batches.forEach(txs => txs.forEach(tx =>res.push(tx)));
  return res;
}

async function getRawTransactionsHelper(txids: string[]): Promise<string[]> {
  let batchCall = () => {
    txids.forEach(txid => rpc.getRawTransaction(txid));
  }
  return new Promise<any>((resolve, reject) => {
    rpc.batch(batchCall, (err, rawtxs) => {
      if (err) reject(err)
      else if (rawtxs.length > 0 && rawtxs[0].error) reject(rawtxs[0].error.message)
      else resolve(rawtxs.map(rawtx => rawtx.result));
    });
  });  
}

async function getRawTransactions(txids: string[]): Promise<string[]> {
  let res: string[] = [];
  let from = 0;
  let promises = [];
  while (from < txids.length) {
    promises.push(getRawTransactionsHelper(txids.slice(from, from+rpc_batch_size)));//To avoid HTTP 413 error

    if (promises.length > 500) {
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

//let utxoCache: Map<string, {value: number, addresses: string[]}> = new Map();

class attachTransactons extends Transform {
  constructor() {
    super({
      objectMode: true,
      //highWaterMark: 256,
      transform: async (block: Block, encoding, callback) => {
        let rawtxs = await getRawTransactions(block.tx);
        let txs: Transaction[] = await decodeRawTransactions(rawtxs);
        txs.forEach(tx => {
          tx.vout.forEach(vout => {
            outputCache.set(tx.txid+";"+vout.n, {value:vout.value, addresses: vout.scriptPubKey.addresses});
          });
        });
        this.push(new BlockWithTransactions(block, txs));
        callback();
      }
    });
  }
}

let cacheHits: number = 0;
let cacheMisses: number = 0;

class attachInputs extends Transform {
  
  constructor() {
    super({
      objectMode: true,
      //highWaterMark: 256,
      transform: async (block: BlockWithTransactions, encoding, callback) => {
        let input_txids = [];
        block.tx.forEach((tx, n) => {
          tx.vin.forEach(vin => {
            if (vin.coinbase) return;
            if (vin.value === undefined) {
              let utxo: {addresses: string[], value: number};
              if (outputCache.has(vin.txid+";"+vin.vout)) {
                cacheHits++;
                utxo = outputCache.peek(vin.txid+";"+vin.vout);
                outputCache.del(vin.txid+";"+vin.vout);
              } else {
                cacheMisses++;
                let foundTx = block.tx.slice(0, n).find(tx => tx.txid === vin.txid);
                if (foundTx !== undefined) {
                  utxo = {
                    addresses: foundTx.vout[vin.vout].scriptPubKey.addresses,
                    value: foundTx.vout[vin.vout].value
                  };
                }  
              }
              if (utxo !== undefined) {
                vin.value = utxo.value;
                if (utxo.addresses && utxo.addresses.length === 1) vin.address = utxo.addresses[0];
              } else {
                if (input_txids.indexOf(vin.txid) >= 0) return;
                input_txids.push(vin.txid);
              }
            }
          });
        });
        if (input_txids.length > 0) {
          console.log("attaching inputs...", input_txids.length);
          let txs2 = await decodeRawTransactions(await getRawTransactions(input_txids));
          block.tx.forEach(tx => {
            tx.vin.forEach(vin => {
              if (vin.coinbase) return;
              if (vin.value === undefined) {
                let index = input_txids.indexOf(vin.txid);
                vin.value = txs2[index].vout[vin.vout].value;
                let pubkey = txs2[index].vout[vin.vout].scriptPubKey;
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


class BlockReader extends Readable {
  currentHash: string;
  currentHeight: number;
  constructor(hash: string, stopHeight: number) {
    super({
      objectMode: true,
      //highWaterMark: 256,
      read: async (size) => {
        if (this.currentHeight !== undefined && this.currentHeight > stopHeight) 
          this.push(null);
        else while (true) {
          let block: Block = await getBlockByHash(this.currentHash);
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
        callback(null);
      }
    });
  } else {
    setTimeout(doProcessing, 10000);
    return;
  }
  if (startHeight === 0) outputCache.reset();
  let startHash: string = await blockService.getRpcBlockHash(startHeight);
  let blockReader = new BlockReader(startHash, toHeight);
  let txAttacher = new attachTransactons();
  let inputAttacher = new attachInputs();
  blockReader.pipe(txAttacher).pipe(inputAttacher).pipe(blockWriter);
  blockReader.on('end', () => {
  });
  blockWriter.on('finish', () => {
    setTimeout(doProcessing, 0);
  });
  setInterval(()=>{
    console.log("blockReader",blockReader.readableLength);
    console.log("txAttacher", txAttacher.readableLength, txAttacher.writableLength);
    console.log("inputAttacher", inputAttacher.readableLength, inputAttacher.writableLength);
    console.log("blockWriter", blockWriter.writableLength);
    console.log("cacheHit rate: "+ cacheHits/(cacheHits+cacheMisses) );
    console.log("utxocache length", outputCache.length);
  }, 5000);
}