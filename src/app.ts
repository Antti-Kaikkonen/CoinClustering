import RpcClient, { RpcClientOptions } from 'bitcoind-rpc';
import encoding from 'encoding-down';
import express from 'express';
import levelup from 'levelup';
import rocksdb from 'rocksdb';
import { Readable, Writable } from 'stream';
import { ClusterController } from './app/controllers/cluster-controller';
import { BlockService } from './app/services/block-service';
import { ClusterAddressService } from './app/services/cluster-address-service';
import { ClusterBalanceService } from './app/services/cluster-balance-service';
import { db_address_cluster_prefix } from './app/services/db-constants';

const config: RpcClientOptions = require('../config');

var rpc = new RpcClient(config);

let db = levelup(encoding(rocksdb('./db')));

let clusterBalanceService = new ClusterBalanceService(db);

let clusterAddressService = new ClusterAddressService(db);

let blockService = new BlockService(db, rpc);

let clusterController = new ClusterController(clusterBalanceService, clusterAddressService);

const app = express();
app.get("/hello", clusterController.clusterCurrentBalances);
app.get("/hello2", clusterController.clusterTransactions);
app.get("/hello3", clusterController.clusterAddresses);
app.get('/cluster_addresses/:id', clusterController.clusterAddresses);
app.listen(3006);


function isMixingTx(tx) {
  if (tx.vin.length < 2) return false;
  if (tx.vout.length !== tx.vin.length) return false;
  let firstInput = tx.vin[0];
  if (typeof firstInput.valueSat !== 'number') return false;
  if (!tx.vin.every(vin => vin.valueSat === firstInput.valueSat)) return false;
  if (!tx.vout.every(vout => vout.valueSat === firstInput.valueSat)) return false;
  return true;
}

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

const blockWriter = new Writable({
  objectMode: true,
  highWaterMark: 4,
  write: async (block, encoding, callback) => {
    await saveBlock(block);
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

function getTransactionAddressBalanceChanges(tx): Map<string, number> {
  let addressToDelta = new Map<string, number>();
  tx.vin.filter(vin => vin.address)
  .forEach(vin => {
    let oldBalance = addressToDelta.get(vin.address);
    if (!oldBalance) oldBalance = 0;
    addressToDelta.set(vin.address, oldBalance-vin.valueSat);
  }); 
  tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1)
  .forEach(vout => {
    let oldBalance = addressToDelta.get(vout.scriptPubKey.addresses[0]);
    if (!oldBalance) oldBalance = 0;
    addressToDelta.set(vout.scriptPubKey.addresses[0], oldBalance+vout.valueSat);
  });
  return addressToDelta;
}

async function addressBalanceChangesToClusterBalanceChanges(addressToDelta: Map<string, number>): Promise<Map<string, number>> {
  let promises = [];
  let addresses = [];
  addressToDelta.forEach((delta: number, address: string) => {
    addresses.push(address);
    promises.push(db.get(db_address_cluster_prefix+address));
  });
  let clusterIds = await Promise.all(promises);
  let clusterToDelta = new Map<string, number>();
  addresses.forEach((address: string, index: number) => {
    let clusterId = clusterIds[index];
    let oldBalance = clusterToDelta.get(clusterId);
    let addressDelta = addressToDelta.get(address);
    if (!oldBalance) oldBalance = 0;
    clusterToDelta.set(clusterId, oldBalance+addressDelta);
  });
  return clusterToDelta;
}

function txAddressesToCluster(tx): Set<string> {
  let result = new Set<string>();
  if (isMixingTx(tx)) return result;
  tx.vin.map(vin => vin.address).filter(address => address !== undefined).forEach(address => result.add(address));
  return result;
}

function txAddresses(tx): Set<string> {
  let result = new Set<string>();
  tx.vin.map(vin => vin.address).filter(address => address !== undefined).forEach(address => result.add(address));
  tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1)
  .map(vout => vout.scriptPubKey.addresses[0]).forEach(address => result.add(address));
  return result;
}

async function getAddressClusterInfo(address: string) {
  return new Promise<{address: string, cluster?: {id: number}}>((resolve, reject) => {
    db.get(db_address_cluster_prefix+address, (error, clusterId: string) => {
      if (clusterId !== undefined) {
        resolve( { address: address, cluster: { id: Number(clusterId) }});
      } else {
        resolve({address: address});
      }
    });
  });
}

async function saveBlock(block) {
  if (block.height%1000 === 0) console.log(block.height);
  //let rawtxs = await getRawTransactions(block.tx);
  let txs = block.tx;//await decodeRawTransactions(rawtxs);
  //if (true) return;
  for (const [txindex, tx] of txs.entries()) {
    let allAddresses = txAddresses(tx);
    let addressesToCluster = txAddressesToCluster(tx);
    let addressesNotToCluster = new Set([...allAddresses].filter(x => !addressesToCluster.has(x)));

    let addressToClusterPromises = [];

    for (let address of allAddresses) {
      addressToClusterPromises.push(getAddressClusterInfo(address));
    }
    let addressesWithClusterInfo = await Promise.all(addressToClusterPromises);

    let singleAddressClustersToCreate = addressesWithClusterInfo
    .filter(v => v.cluster === undefined && addressesNotToCluster.has(v.address))
    .map(v => [v.address]);
    await clusterAddressService.createMultipleAddressClusters(singleAddressClustersToCreate);

    let addressesWithClustersToCluster = addressesWithClusterInfo.filter(v => addressesToCluster.has(v.address));

    let clusterIds: number[] = Array.from(new Set( addressesWithClustersToCluster.filter(v => v.cluster !== undefined).map(v => v.cluster.id) ));

    let nonClusterAddresses = addressesWithClustersToCluster.filter(v => v.cluster === undefined).map(v => v.address);

    if (clusterIds.length === 0) {
      if (nonClusterAddresses.length > 0) {
        console.log("creating cluster with "+nonClusterAddresses.length+" addresses");
        await clusterAddressService.createClusterWithAddresses(nonClusterAddresses);
      }  
    } else {
      let toCluster: number = Math.min(...clusterIds);
      let fromClusters: number[] = clusterIds.filter(clusterId => clusterId !== toCluster);
      if (fromClusters.length > 0) console.log("merging to",toCluster, "from ", fromClusters.join(","));
      if (fromClusters.length > 0) {
        await clusterAddressService.mergeClusterAddresses(toCluster, ...fromClusters);
        await clusterBalanceService.mergeClusterTransactions(toCluster, ...fromClusters);
      }

      if (nonClusterAddresses.length > 0) {
        console.log("adding addresses", nonClusterAddresses, "to cluster", toCluster);
        await clusterAddressService.addAddressesToCluster(nonClusterAddresses, toCluster);
      }   
    }
    let addressBalanceChanges = getTransactionAddressBalanceChanges(tx);
    let clusterBalanceChanges = await addressBalanceChangesToClusterBalanceChanges(addressBalanceChanges);
    await clusterBalanceService.saveClusterBalanceChanges(tx.txid, block.height, txindex, clusterBalanceChanges);
  }
  await blockService.saveBlockHash(block.height, block.hash);
}

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