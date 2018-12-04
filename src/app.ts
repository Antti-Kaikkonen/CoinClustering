import encoding from 'encoding-down';
import express from 'express';
import leveldown from 'leveldown';
import levelup from 'levelup';
import { Readable, Writable } from 'stream';

import { ClusterController } from './app/controllers/cluster-controller';
import { ClusterAddressService } from './app/services/cluster-address-service';
import { ClusterBalanceService } from './app/services/cluster-balance-service';

const config = require('../config');
const RpcClient = require('bitcoind-rpc');

var rpc = new RpcClient(config);

let db = levelup(encoding(leveldown('./db')));

let clusterBalanceService = new ClusterBalanceService(db);

let clusterAddressService = new ClusterAddressService(db);

let clusterController = new ClusterController(clusterBalanceService, clusterAddressService);

const app = express();
app.get("/hello", clusterController.clusterCurrentBalances);
app.get("/hello2", clusterController.clusterTransactions);
app.get("/hello3", clusterController.clusterAddresses);
app.get('/cluster_addresses/:id', clusterController.clusterAddresses);
app.listen(3006);

const db_cluster_address_count_prefix = "cluster_address_count/";//prefix/clusterId => count
const db_address_cluster_prefix = "address_cluster/";//prefix/address => clusterId



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
  highWaterMark: 64,
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
      highWaterMark: 64,
      read: async (size) => {
        let block = await getBlockByHash(this.currentHash);
        this.currentHash = block.nextblockhash;
        this.push(block);
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
  return new Promise<{address: string, cluster?: {size: number, id: string}}>((resolve, reject) => {
    db.get(db_address_cluster_prefix+address, (error, clusterId: string) => {
      if (clusterId !== undefined) {
        db.get(db_cluster_address_count_prefix+clusterId, (error, clusterSize) => {
          resolve(
            { address: address, cluster: { size: clusterSize, id: clusterId } }
          );
        });
      } else {
        resolve({address: address});
      }
    });
  });
}

async function saveBlock(block) {
  if (block.height%1000 === 0) console.log(block.height);
  //currentHash = block.nextblockhash;
  let rawtxs = await getRawTransactions(block.tx);
  let txs = await decodeRawTransactions(rawtxs);
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
    /*for (let address of singleAddressClustersToCreate) {
      await clusterAddressService.createClusterWithAddresses([address]);//TODO batch
    }*/
    //await Promise.all(singleAddressClustersToCreate.map(address => clusterAddressService.createClusterWithAddresses([address])));//TODO: use db.batch()

    let addressesWithClustersToCluster = addressesWithClusterInfo.filter(v => addressesToCluster.has(v.address));

    let biggestCluster = addressesWithClustersToCluster.filter(e => e.cluster !== undefined).reduce((a, b) => {
      if (a.cluster.size > b.cluster.size) {
        return a;
      } else {
        return b;
      }  
    }, {cluster:{size:0}});

    let clusterIds = Array.from(new Set( addressesWithClustersToCluster.filter(v => v.cluster !== undefined).map(v => v.cluster.id) ));
    let nonClusterAddresses = addressesWithClustersToCluster.filter(v => v.cluster === undefined).map(v => v.address);

    if (biggestCluster.cluster.id === undefined) {
      await clusterAddressService.createClusterWithAddresses(nonClusterAddresses);
    } else {
      let fromClusters = clusterIds.filter(clusterId => clusterId !== biggestCluster.cluster.id);
      if (fromClusters.length > 0) console.log("merging to",biggestCluster.cluster.id, "from ", fromClusters.join(","));
      for (let fromCluster of fromClusters) {
        await clusterAddressService.mergeClusterAddresses(fromCluster, biggestCluster.cluster.id);
        await clusterBalanceService.mergeClusterTransactions(fromCluster, biggestCluster.cluster.id);
      }
      if (nonClusterAddresses.length > 0) console.log("nca", nonClusterAddresses);
      await clusterAddressService.addAddressesToCluster(nonClusterAddresses, biggestCluster.cluster.id);
    }
    let addressBalanceChanges = getTransactionAddressBalanceChanges(tx);
    let clusterBalanceChanges = await addressBalanceChangesToClusterBalanceChanges(addressBalanceChanges);
    await clusterBalanceService.saveClusterBalanceChanges(tx.txid, block.height, txindex, clusterBalanceChanges);
  }
}


rpc.getBlockHash(1, (error, ret) => {
  let blockReader = new BlockReader(ret.result);
  blockReader.pipe(blockWriter);
});
