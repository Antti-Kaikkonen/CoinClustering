import cors from 'cors';
import express from 'express';
import { Writable } from 'stream';
import { AddressController } from './app/controllers/address-controller';
import { ClusterController } from './app/controllers/cluster-controller';
import { TransactionController } from './app/controllers/transaction-controller';
import { myContainer } from "./app/inversify.config";
import RpcApi from './app/misc/rpc-api';
import { BlockWithTransactions } from './app/models/block';
import addressRoutes from './app/routes/address';
import clusterRoutes from './app/routes/cluster';
import transactionRoutes from './app/routes/transaction';
import { BinaryDB } from './app/services/binary-db';
import { BlockImportService } from './app/services/block-import-service';
import { BlockchainReader } from './app/services/blockchain-reader';



//export NODE_OPTIONS="--max_old_space_size=8192"

//export NODE_OPTIONS="--max_old_space_size=16384"

let cwd = process.cwd();
const config: any = require(cwd+'/config');

let rpcApi = myContainer.get(RpcApi);//new RpcApi(config.host, config.port, config.user, config.pass);

let db: BinaryDB = myContainer.get(BinaryDB);

let clusterController = myContainer.get(ClusterController);//null;//new ClusterController(db, addressEncodingService, rpcApi);

let addressController = myContainer.get(AddressController);//null;//new AddressController(db, addressEncodingService);

let transactionController = myContainer.get(TransactionController);//null;//new TransactionController(db, addressEncodingService, rpcApi);

let blockImportService = myContainer.get(BlockImportService);//null;//new BlockImportService(db, clusterAddressService, clusterBalanceService, addressService, addressEncodingService);

let blockchainReader = myContainer.get(BlockchainReader);//new BlockchainReader(restApi, rpcApi, addressEncodingService, db);

const app = express();
app.use(cors());

app.use('/clusters', clusterRoutes(clusterController));
app.use('/addresses', addressRoutes(addressController));
app.use('/tx', transactionRoutes(transactionController));
app.listen(config.listen_port);

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
      write: async (block: BlockWithTransactions, encoding, callback) => {
        await blockImportService.blockMerging(block);
        callback(null);
      }
    });
  } else if (lastSavedTxHeight < height-stay_behind_blocks) {
    startHeight = lastSavedTxHeight > -1 ? lastSavedTxHeight + 1 : 1;
    toHeight = lastMergedHeight;
    console.log("saving transactions between blocks", startHeight, "and", toHeight);
    blockWriter = new Writable({
      objectMode: true,
      write: async (block: BlockWithTransactions, encoding, callback) => {
        await blockImportService.saveBlockTransactionsAsync(block);
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