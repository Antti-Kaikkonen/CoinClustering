import cors from 'cors';
import EncodingDown from 'encoding-down';
import express from 'express';
import rocksDB from 'rocksdb';
import { Writable } from 'stream';
import { ClusterController } from './app/controllers/cluster-controller';
import { BlockWithTransactions } from './app/models/block';
import clusterRoutes from './app/routes/cluster';
import { AddressEncodingService } from './app/services/address-encoding-service';
import { BinaryDB } from './app/services/binary-db';
import { BlockImportService } from './app/services/block-import-service';
import { BlockchainReader } from './app/services/blockchain-reader';
import { ClusterAddressService } from './app/services/cluster-address-service';
import { ClusterTransactionService } from './app/services/cluster-transaction-service';
import { OutputCacheTable } from './app/tables/output-cache-table';
import RestApi from './app/utils/rest-api';
import RpcApi from './app/utils/rpc-api';



//export NODE_OPTIONS="--max_old_space_size=8192"

//export NODE_OPTIONS="--max_old_space_size=16384"

let cwd = process.cwd();
const config: any = require(cwd+'/config');

let addressEncodingService = new AddressEncodingService(config.pubkeyhash, config.scripthash, config.segwitprefix);


//var rpc = new RpcClient(config);
let rocksdb = rocksDB(cwd+'/rocksdb');

let restApi = new RestApi(config.host, config.port);
let rpcApi = new RpcApi(config.host, config.port, config.user, config.pass);

let db = new BinaryDB(EncodingDown<Buffer, Buffer>(rocksdb, {keyEncoding: 'binary', valueEncoding: 'binary'}), {
  writeBufferSize: 16 * 1024 * 1024,
  cacheSize: config.dbcache * 1024 * 1024,
  compression: true
});

let clusterBalanceService = new ClusterTransactionService(db);

let clusterAddressService = new ClusterAddressService(db, addressEncodingService);

let clusterController = new ClusterController(db, addressEncodingService, rpcApi);

let blockImportService = new BlockImportService(db, clusterAddressService, clusterBalanceService, addressEncodingService);

let outputCacheTable = new OutputCacheTable(db, addressEncodingService);

let blockchainReader = new BlockchainReader(restApi, rpcApi, addressEncodingService, db);

const app = express();
app.use(cors());

app.use('/cluster', clusterRoutes(clusterController));
app.listen(config.listen_port);




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

const stay_behind_blocks = 100;

async function doProcessing() {
  await db.writeBatchService.process();
  let height = await rpcApi.getRpcHeight();
  console.log("rpc height", height);
  let lastMergedHeight: number = await blockImportService.getLastMergedHeight();
  let lastSavedTxHeight: number = await blockImportService.getLastSavedTxHeight();
  console.log("last saved tx height", lastSavedTxHeight);
  let blockWriter: Writable;
  let startHeight: number;
  let toHeight: number;
  if (lastMergedHeight < height-stay_behind_blocks) {
    startHeight = lastMergedHeight > -1 ? lastMergedHeight + 1 : 1;
    toHeight = height-stay_behind_blocks;
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
  } else if (lastSavedTxHeight < height-stay_behind_blocks) {
    startHeight = lastSavedTxHeight > -1 ? lastSavedTxHeight + 1 : 1;
    toHeight = lastMergedHeight;
    console.log("saving transactions between blocks", startHeight, "and", toHeight);
    blockWriter = new Writable({
      objectMode: true,
      //highWaterMark: 256,
      write: async (block: BlockWithTransactions, encoding, callback) => {
        await blockImportService.saveBlockTransactionsAsync(block);
        deleteBlockInputs(block);
        callback(null);
      }
    });
  } else {
    setTimeout(doProcessing, 10000);
    return;
  }

  let blockReader = blockchainReader.createReadStream(startHeight, toHeight);
  blockReader.pipe(blockWriter);

  let interval = setInterval(()=>{
    console.log("blockWriter", blockWriter.writableLength);
  }, 5000);
  
  blockWriter.on('finish', () => {
    clearInterval(interval);
    setTimeout(doProcessing, 0);
  });
}

doProcessing();