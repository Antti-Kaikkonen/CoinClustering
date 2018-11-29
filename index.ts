import encoding from 'encoding-down';
import leveldown from 'leveldown';
import levelup from 'levelup';
import { Readable, Writable } from 'stream';

const config = require('./config');
const RpcClient = require('bitcoind-rpc');

var rpc = new RpcClient(config);

let db = levelup(encoding(leveldown('./db')));

const db_cluster_address_prefix = "cluster_address/";//prefix/clusterId/# => address (no particular order)
const db_cluster_address_count_prefix = "cluster_address_count/";//prefix/clusterId => count
const db_address_cluster_prefix = "address_cluster/";//prefix/address => clusterId

const db_cluster_balance_prefix = "cluster_balance_event/";// prefix/clusterid/# => txid;balanceAfter (order # by 1:height, 2:index in block)
const db_cluster_tx_balance_prefix = "cluster_tx_balance/";//prefix/clusterid/txid => #;balanceAfter
const db_cluster_balance_count_prefix = "cluster_balance_count/";


const db_address_balance_prefix = "address_balance/";// prefix/address/# => txid;balanceAfter (order # by 1:height, 2:index in block)
const db_address_tx_balance_prefix = "address_tx_balance/";// prefix/address/txid => balanceAfter
const db_address_balance_count_prefix = "address_balance_count/";

const db_next_cluster_id = "next_cluster_id/";
const db_value_separator = ";";

let next_cluster_id: number;

//Adds leading zeros to make result 14 characters long for lexicographical ordering. Only works for integers from 0 to 99999999999999
function integer2LexString(number: number): string {
  let result = ""+number;
  while (result.length < 14) {
    result = "0"+result;
  }
  return result;
}

function lexString2Integer(lexString: string): number {
  while (lexString.startsWith('0')) {
    lexString = lexString.substr(1);
  }
  return Number(lexString);
}

function isMixingTx(tx) {
  if (tx.vin.length < 2) return false;
  if (tx.vout.length !== tx.vin.length) return false;
  let firstInput = tx.vin[0];
  if (typeof firstInput.valueSat !== 'number') return false;
  if (!tx.vin.every(vin => vin.valueSat === firstInput.valueSat)) return false;
  if (!tx.vout.every(vout => vout.valueSat === firstInput.valueSat)) return false;
  return true;
}

async function mergeClusterTransactions(fromCluster: string, toCluster: string) {
  let p1 = new Promise<{txid: string, balance: number, height: number, n: number}[]>((resolve, reject) => {
    let transactions = [];
    db.createValueStream({
      gte:db_cluster_balance_prefix+fromCluster+"/0",
      lt:db_cluster_balance_prefix+fromCluster+"/z"
    }).on("data", function(data) {
      let components = data.split(db_value_separator);
      transactions.push({ txid: components[0], balance:Number(components[1]), height:Number(components[2]), n:Number(components[3]) });
    })
    .on('error', function (err) {
      reject(err);
    })
    .on('close', function () {
      resolve(transactions);
    })
    .on('end', function () {
    });
  })
  
  let p2 = new Promise<{txid: string, balance: number, height: number, n: number}[]>((resolve, reject) => {
    let transactions = [];
    db.createValueStream({
      gte:db_cluster_balance_prefix+toCluster+"/0",
      lt:db_cluster_balance_prefix+toCluster+"/z"
    }).on("data", function(data) {
      let components = data.split(db_value_separator);
      transactions.push({ txid: components[0], balance:Number(components[1]), height:Number(components[2]), n:Number(components[3]) });
    })
    .on('error', function (err) {
    })
    .on('close', function () {
      resolve(transactions);
    })
    .on('end', function () {
    });
  });
  let values = await Promise.all([p1, p2]);
  let transactionsFrom = values[0].map((value, index, arr) => { 
    let delta = index === 0 ? value.balance : value.balance-arr[index-1].balance;
    return {txid: value.txid, delta: delta, height: value.height, n: value.n};
  });

  let transactionsTo = values[1].map((value, index, arr) => { 
    let delta = index === 0 ? value.balance : value.balance-arr[index-1].balance;
    return {txid: value.txid, delta: delta, height: value.height, n: value.n};
  });


  let combined = [];
  let i1 = 0;
  let i2 = 0;

  //Merge two sorted arrays into one sorted array
  while (i1 < transactionsFrom.length || i2 < transactionsTo.length) {
    if (
      i2 < transactionsTo.length &&
        (i1 === transactionsFrom.length || 
        transactionsTo[i2].height < transactionsFrom[i1].height || 
        (
          transactionsTo[i2].height === transactionsFrom[i1].height && 
          transactionsTo[i2].n < transactionsFrom[i1].n
        )
      )
    ) 
    {
      combined.push(transactionsTo[i2]);
      i2++;
    } else {
      combined.push(transactionsFrom[i1]);
      i1++;
    } 
  }
  let balances = [];
  balances[0] = combined[0].delta;
  for (let i = 1; i < combined.length; i++) {
    balances[i] = balances[i-1]+combined[i].delta;
  }

  let ops = [];
  combined.forEach((tx, index) => ops.push({
    type: "put",
    key:db_cluster_balance_prefix+toCluster+"/"+integer2LexString(index),
    value: cluster_balance_value(tx.txid, tx.delta, tx.height, tx.n)
  }));
  transactionsFrom.forEach((tx, index) => {
    ops.push({
      type: "del",
      key:db_cluster_balance_prefix+fromCluster+"/"+integer2LexString(index)
    });
  });
  ops.push({
    type:"put",
    key:db_cluster_balance_count_prefix+toCluster,
    value:combined.length
  });
  ops.push({
    type:"del",
    key:db_cluster_balance_count_prefix+fromCluster
  });

  return new Promise<any>(async (resolve, reject) => {
    db.batch(ops, function(error) {
      if (error) reject(error)
      else resolve();
    });
  });

}

async function mergeClusterAddresses(fromClusterId: string, toClusterId: string) {

  let addressCountPromise = new Promise<any>(function(resolve, reject) {
    db.get(db_cluster_address_count_prefix+toClusterId, (error, count) => {
      if (error) reject(error)
      else resolve(count);
    });
  });

  let addressesPromise = new Promise<any>((resolve, reject) => {
    let addresses: string[] = [];
    db.createValueStream({
      gte:db_cluster_address_prefix+fromClusterId+"/0",
      lt:db_cluster_address_prefix+fromClusterId+"/z"
    })
    .on('data', function (data) {
      addresses.push(data);
    })
    .on('error', function (err) {
      reject(err);
    })
    .on('close', function () {
      resolve(addresses);
    })
    .on('end', function () {
    });
  });

  let values = await Promise.all([addressCountPromise, addressesPromise]);
  let ops = [];
  let count = values[0];
  let addresses = values[1];
  addresses.forEach((address, index) => {
    let newIndex = count+index+1;
    ops.push({
      type:"put", 
      key: db_cluster_address_prefix+toClusterId+"/"+integer2LexString(newIndex), 
      value:address
    });
    ops.push({
      type:"del", 
      key: db_cluster_address_prefix+fromClusterId+"/"+integer2LexString(index)
    });
    ops.push({
      type:"put",
      key:db_address_cluster_prefix+address,
      value:toClusterId
    });
  });
  ops.push({
    type:"put", 
    key: db_cluster_address_count_prefix+toClusterId, 
    value: count+addresses.length
  });
  ops.push({
    type:"del", 
    key: db_cluster_address_count_prefix+fromClusterId
  });

  return db.batch(ops);
}

async function createClusterWithAddresses(addresses: string[]) {
  return new Promise<any>((resolve, reject) => {
    let clusterId = next_cluster_id;
    next_cluster_id++;
    if (addresses.length === 0) resolve();
    let ops = [];
    ops.push({
      type:"put",
      key:db_next_cluster_id,
      value:next_cluster_id
    });
    ops.push({
      type:"put",
      key:db_cluster_address_count_prefix+clusterId,
      value:addresses.length
    });
    addresses.forEach((address, index) => {
      ops.push({
        type: "put",
        key: db_cluster_address_prefix+clusterId+"/"+integer2LexString(index),
        value: address
      });
      ops.push({
        type: "put",
        key: db_address_cluster_prefix+address,
        value: clusterId
      });
    });
    db.batch(ops, function(error) {
      if (error) reject(error)
      else resolve();
    });
  })
}

async function addAddressesToCluster(addresses: string[], clusterId: string) {
  return new Promise<any>((resolve, reject) => {
    if (addresses.length === 0) resolve();
    db.get(db_cluster_address_count_prefix+clusterId, (error, count) => {
      let ops = [];
      ops.push({
        type:"put",
        key:db_cluster_address_count_prefix+clusterId,
        value:count+addresses.length
      });
      addresses.forEach((address, index) => {
        let newIndex = count+index+1;
        ops.push({
          type: "put",
          key: db_cluster_address_prefix+clusterId+"/"+integer2LexString(newIndex),
          value: address
        });
        ops.push({
          type: "put",
          key: db_address_cluster_prefix+address,
          value: clusterId
        });
      });
      db.batch(ops, (error) => {
        if (error) reject(error)
        else resolve();
      });
    });
  });
}

async function getBlockByHash(hash: string) {
  return new Promise<any>((resolve, reject) => {
    rpc.getBlock(hash, (error, ret) => {
      if (error) reject(error)
      else resolve(ret.result);
    });  
  });
}

async function decodeRawTransaction(rawtx) {
  return new Promise<any>((resolve, reject) => {
    rpc.decodeRawTransaction(rawtx, (error, ret) => {
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

async function getRawTransaction(txid: string) {
  return new Promise<any>((resolve, reject) => {
    rpc.getRawTransaction(txid, function(error, ret) {
      if (error) reject(error)
      else resolve(ret.result);
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



db.get(db_next_cluster_id, (error, value) => {
  if (value) {
    next_cluster_id = value;
  } else {
    next_cluster_id = 0;
  }
  rpc.getBlockHash(1, (error, ret) => {
    let blockReader = new BlockReader(ret.result);
    blockReader.pipe(blockWriter);
  });
});

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


async function getCurrentClusterBalance(clusterId: string) {
  return new Promise<{txid: string, balance: number, index: number}>((resolve, reject) => {
    let result;
    db.createReadStream({
      gte:db_cluster_balance_prefix+clusterId+"/0",
      lt:db_cluster_balance_prefix+clusterId+"/z",
      reverse: true,
      limit: 1
    })
    .on('data', function (data) {
      let key: string = data.key;
      let index = Number(key.substr((db_cluster_balance_prefix+clusterId+"/").length));
      let value: string = data.value;
      let valueComponents = value.split(db_value_separator);
      
      result = {txid:valueComponents[0], balance:valueComponents[1]};
    })
    .on('error', function (err) {
      reject(err);
    })
    .on('close', function () {
      resolve(result);
    })
    .on('end', function () {
    });
  });
}

async function getCurrentAddressBalance(address: string) {
  return new Promise<number>((resolve, reject) => {
    let result = 0;
    db.createReadStream({
      gte:db_address_balance_prefix+address+"/0",
      lt:db_address_balance_prefix+address+"/z",
      reverse: true,
      limit: 1
    })
    .on('data', function (data) {
      result = data;
    })
    .on('error', function (err) {
      reject(err);
    })
    .on('close', function () {
      resolve(result);
    })
    .on('end', function () {
    });
  });
}

function cluster_balance_value(txid: string, balance: number, height: number, n: number) {
  return txid+db_value_separator+balance+db_value_separator+height+db_value_separator+n;
}

function cluster_tx_balance_value(index: number, balance: number, height: number, n: number) {
  return index+db_value_separator+balance+db_value_separator+height+db_value_separator+n;
}

async function saveClusterBalanceChanges(txid: string, height: number, n: number, clusterIdToDelta: Map<string, number>) {
  let promises = [];
  let clusterIds = [];
  let deltas = [];
  clusterIdToDelta.forEach((delta: number, clusterId: string) => {
    clusterIds.push(clusterId);
    promises.push(getCurrentClusterBalance(clusterId));
    deltas.push(delta);
  });
  let oldBalances = Promise.all(promises);
  let ops = [];
  for (let i = 0; i < deltas.length; i++) {
    let clusterId = clusterIds[i];
    let index = oldBalances[i] === undefined ? 0 : oldBalances[i].index+1;
    let oldBalance = oldBalances[i] === undefined ? 0 : oldBalances[i].balance;
    let newBalance = oldBalance+deltas[i];
    ops.push({
      type:"put",
      key:db_cluster_balance_prefix+clusterId+"/"+integer2LexString(index),
      value:cluster_balance_value(txid, newBalance, height, n)
    });
    ops.push({
      type:"put",
      key:db_cluster_tx_balance_prefix+clusterId+"/"+txid,
      value:cluster_tx_balance_value(index, newBalance, height, n)
    });
  }
  return db.batch(ops);
}

async function saveBlock(block) {
  if (block.height%1000 === 0) console.log(block.height);
  //currentHash = block.nextblockhash;
  let rawtxs = await getRawTransactions(block.tx);
  let txs = await decodeRawTransactions(rawtxs);
  for (const [txindex, tx] of txs.entries()) {
    if (isMixingTx(tx)) continue;
    let inputAddresses = new Set<string>( tx.vin.map(vin => vin.address).filter(address => address !== undefined) );
    let outputAddresses = new Set<string>( 
      tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1)
      .map(vout => vout.scriptPubKey.addresses[0]) 
    );
    let allAddresses = new Set<string>([...inputAddresses, ...outputAddresses]);
    //TODO: Create 1 address cluster for outputs

    let addressToClusterPromises = [];
    for (let address of allAddresses) {
      let promise = new Promise<{address: string, clusterSize: number, clusterId?: string}>((resolve, reject) => {
        db.get(db_address_cluster_prefix+address, (error, clusterId: string) => {
          if (clusterId) {
            db.get(db_cluster_address_count_prefix+clusterId, (error, clusterSize) => {
              resolve({address: address, clusterId: clusterId, clusterSize: clusterSize});
            });
          } else {
            resolve({address: address, clusterSize: 0});
          }
        });
      });
      addressToClusterPromises.push(promise);
    }
    let addressToClusterArr = await Promise.all(addressToClusterPromises);
    let addressToClusterInputs = addressToClusterArr.filter(v => inputAddresses.has(v.address));
    let outputAddressesWithoutCluster = addressToClusterArr
    .filter(v => v.clusterId === undefined && outputAddresses.has(v.address) && !inputAddresses.has(v.address))
    .map(v => v.address);
    await Promise.all(outputAddressesWithoutCluster.map(address => createClusterWithAddresses([address])));

    let biggestCluster = addressToClusterInputs.reduce((a, b) => {
      if (a.clusterSize > b.clusterSize) {
        return a;
      } else {
        return b;
      }  
    }, {clusterSize:0});

    let clusterIds = Array.from(new Set( addressToClusterInputs.map(v => v.clusterId).filter(clusterId => clusterId != undefined) ));
    let nonClusterAddresses = addressToClusterInputs.filter(v => v.clusterId === undefined).map(v => v.address);

    if (biggestCluster.clusterId === undefined) {
      await createClusterWithAddresses(nonClusterAddresses);
    } else {
      let fromClusters = clusterIds.filter(clusterId => clusterId !== biggestCluster.clusterId);
      for (let fromCluster of fromClusters) {
        //console.log("merging cluster "+ fromCluster +" to "+biggestCluster.clusterId);
        await mergeClusterAddresses(fromCluster, biggestCluster.clusterId);
        await mergeClusterTransactions(fromCluster, biggestCluster.clusterId);
      }
      await addAddressesToCluster(nonClusterAddresses, biggestCluster.clusterId);
    }
    let addressBalanceChanges = getTransactionAddressBalanceChanges(tx);
    //TODO save addressBalanceChanges
    let clusterBalanceChanges = await addressBalanceChangesToClusterBalanceChanges(addressBalanceChanges);
    await saveClusterBalanceChanges(tx.txid, block.height, txindex, clusterBalanceChanges);
    //TODO save clusterBalanceChanges
  }
}
