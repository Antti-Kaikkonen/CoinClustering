const level = require('level');
var RpcClient = require('bitcoind-rpc');
const config = require('./config');



var rpc = new RpcClient(config);

const db = level('./database', 
{ 
  valueEncoding: 'json', 
  cacheSize: 128*1024*1024, 
  blockSize: 4096, 
  writeBufferSize: 4*1024*1024 
});


const db_cluster_transaction_prefix = "cluster_tx/";
const db_cluster_address_prefix = "cluster_address/";
const db_cluster_transaction_count_prefix = "cluster_tx_count/";
const db_cluster_address_count_prefix = "cluster_address_count/";
const db_address_cluster_prefix = "address_cluster/";
const db_next_cluster_id = "next_cluster_id/";

let next_cluster_id;


function isMixingTx(tx) {
  if (tx.vin.length < 2) return false;
  if (tx.vout.length !== tx.vin.length) return false;
  let firstInput = tx.vin[0];
  if (typeof firstInput.valueSat !== 'number') return false;
  if (!tx.vin.every(vin => vin.valueSat === firstInput.valueSat)) return false;
  if (!tx.vout.every(vout => vout.valueSat === firstInput.valueSat)) return false;
  return true;
}

async function mergeClusters(fromClusterId, toClusterId) {

  let addressCountPromise = new Promise(function(resolve, reject) {
    db.get(db_cluster_address_count_prefix+toClusterId, (error, count) => {
      if (error) reject(error)
      else resolve(count);
    });
  });

  let addressesPromise = new Promise((resolve, reject) => {
    let addresses = [];
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

  return new Promise(async (resolve, reject) => {
    let values = await Promise.all([addressCountPromise, addressesPromise]);
    let ops = [];
    let count = values[0];
    let addresses = values[1];
    addresses.forEach((address, index) => {
      let newIndex = count+index+1;
      ops.push({
        type:"put", 
        key: db_cluster_address_prefix+toClusterId+"/"+newIndex, 
        value:address
      });
      ops.push({
        type:"del", 
        key: db_cluster_address_prefix+fromClusterId+"/"+index
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
      value:count+addresses.length
    });
    ops.push({
      type:"del", 
      key: db_cluster_address_count_prefix+fromClusterId
    });
    db.batch(ops, function(error) {
      if (error) reject(error)
      else resolve();
    });
  });
}

async function createClusterWithAddresses(addresses) {
  return new Promise((resolve, reject) => {
    let clusterId = next_cluster_id;
    next_cluster_id++;
    if (addresses.length === 0) resolve();
    db.get
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
        key: db_cluster_address_prefix+clusterId+"/"+index,
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

async function addAddressesToCluster(addresses, clusterId) {
  return new Promise((resolve, reject) => {
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
          key: db_cluster_address_prefix+clusterId+"/"+newIndex,
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

db.get(db_next_cluster_id, (error, value) => {
  if (value) {
    next_cluster_id = value;
  } else {
    next_cluster_id = 0;
  }
  console.log("clusterID", next_cluster_id);
  process();
});

async function getBlockByHash(hash) {
  return new Promise((resolve, reject) => {
    rpc.getBlock(hash, (error, ret) => {
      if (error) reject(error)
      else resolve(ret.result);
    });  
  });
}

async function decodeRawTransaction(rawtx) {
  return new Promise((resolve, reject) => {
    rpc.decodeRawTransaction(rawtx, function(error, ret) {
      if (error) reject(error)
      else resolve(ret.result);
    });  
  });
}

async function decodeRawTransactions(rawtxs) {

  let batchCall = () => {
    rawtxs.forEach(rawtx => rpc.decodeRawTransaction(rawtx));
  }

  return new Promise((resolve, reject) => {
    rpc.batch(batchCall, (err, txs) => {
      if (err) reject(err)
      else resolve(txs.map(tx => tx.result));
    });
  });

}

async function getRawTransactions(txids) {

  let batchCall = () => {
    txids.forEach(txid => rpc.getRawTransaction(txid));
  }

  return new Promise((resolve, reject) => {
    rpc.batch(batchCall, (err, rawtxs) => {
      if (err) reject(err)
      else resolve(rawtxs.map(rawtx => rawtx.result));
    });
  });  
}

async function getRawTransaction(txid) {
  return new Promise((resolve, reject) => {
    rpc.getRawTransaction(txid, function(error, ret) {
      if (error) reject(error)
      else resolve(ret.result);
    });  
  });
}

async function process() {
  rpc.getBlockHash(1, async (error, ret) => {
    let currentHash = ret.result;
    while (currentHash !== undefined) {
      let block = await getBlockByHash(currentHash);
      if (block.height%1000 === 0) console.log(block.height);
      currentHash = block.nextblockhash;
      let rawtxs = await getRawTransactions(block.tx);
      let txs = await decodeRawTransactions(rawtxs);
      for (let tx of txs) {
        //let rawtx = await getRawTransaction(txid);
        //let tx = await decodeRawTransaction(rawtx);
        if (isMixingTx(tx)) continue;
        let clusterAddresses = Array.from(new Set( tx.vin.map(vin => vin.address).filter(address => address !== undefined) ));
        let promises = [];
        for (let address of clusterAddresses) {
          let promise = new Promise((resolve, reject) => {
            db.get(db_address_cluster_prefix+address, (error, clusterId) => {
              if (clusterId) {
                db.get(db_cluster_address_count_prefix+clusterId, (error, clusterSize) => {
                  resolve({address: address, clusterId: clusterId, clusterSize: clusterSize});
                });
              } else {
                resolve({address: address, clusterSize: 0});
              }
            });
          });
          promises.push(promise);
        }
        let values = await Promise.all(promises);

        
        let biggestCluster = values.reduce((a, b) => {
          if (a.clusterSize > b.clusterSize) {
            return a;
          } else {
            return b;
          }  
        }, {clusterSize:0});


        let clusterIds = Array.from(new Set( values.map(v => v.clusterId).filter(clusterId => clusterId != undefined) ));
        let nonClusterAddresses = values.filter(v => v.clusterId === undefined).map(v => v.address);

        if (biggestCluster.clusterId === undefined) {
          await createClusterWithAddresses(nonClusterAddresses);
        } else {
          let fromClusters = clusterIds.filter(clusterId => clusterId !== biggestCluster.clusterId);
          for (let fromCluster of fromClusters) {
            console.log("merging cluster "+ fromCluster +" to "+biggestCluster.clusterId);
            await mergeClusters(fromCluster, biggestCluster.clusterId);
          }
          await addAddressesToCluster(nonClusterAddresses, biggestCluster.clusterId);
        }
      }
    }
  });

}