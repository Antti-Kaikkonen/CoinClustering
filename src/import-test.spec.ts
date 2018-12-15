import RpcClient from 'bitcoind-rpc';
import { expect } from 'chai';
import EncodingDown from 'encoding-down';
import LevelUp from 'levelup';
import 'mocha';
import rocksdb from 'rocksdb';
import { BlockImportService } from './app/services/block-import-service';
import { BlockService } from './app/services/block-service';
import { ClusterAddressService } from './app/services/cluster-address-service';
import { ClusterBalanceService } from './app/services/cluster-balance-service';

describe('Save a blocks with 3 transactions', () => {


  const dbpath = './test-db';

  let db;

  let clusterAddressService: ClusterAddressService;
  let clusterBalanceService: ClusterBalanceService;

  var rpc = new RpcClient(undefined);

  let blockService: BlockService;

  let blockImportService: BlockImportService;
  before(async () => {
    await new Promise((resolve, reject) => {
      rocksdb['destroy'](dbpath, () => {
        resolve();
      });
    });
    db = LevelUp(EncodingDown(rocksdb(dbpath)), {errorIfExists: true});
    clusterAddressService = new ClusterAddressService(db);
    clusterBalanceService = new ClusterBalanceService(db);
    blockService = new BlockService(db, rpc);
    blockImportService = new BlockImportService(db, clusterAddressService, clusterBalanceService, blockService);

    await blockImportService.saveBlock(b1);
  });

  let b1 = {
    "hash":"000000043657e82e2123aca9d10917f766c41d94efb1243d59d248542d2604af",
    "height":2710,
    "tx":[
      {
        "txid":"tx1",//create balances from coinbase
        "vin":[
          {
            "coinbase":"02960a011e062f503253482f"
          }
        ],
        "vout":[
          {
            "valueSat":10,
            "scriptPubKey":{
              "addresses":[
                  "address1"
              ]
            }
          },
          {
            "valueSat":10,
            "scriptPubKey":{
              "addresses":[
                  "address2"
              ]
            }
          },
          {
            "valueSat":10,
            "scriptPubKey":{
              "addresses":[
                  "address3"
              ]
            }
          },
          {
            "valueSat":10,
            "scriptPubKey":{
              "addresses":[
                  "address4"
              ]
            }
          }
        ]
      },
      {
        "txid":"tx2",//spend from address1 to address2 and address3
        "vin":[
            {
              "valueSat":2,
              "address":"address1"
            }
        ],
        "vout":[
          {
            "valueSat":1,
            "scriptPubKey":{
              "addresses":[
                "address2"
              ]
            }
          },
          {
            "valueSat":1,
            "scriptPubKey":{
              "addresses":[
                "address3"
              ]
            }
          }
        ]
      },
      {
        "txid":"tx3",//send from address3 and address4 to address 2
        "vin":[
            {
              "valueSat":11,
              "address":"address3"
            },
            {
              "valueSat":10,
              "address":"address4"
            }
        ],
        "vout":[
          {
            "valueSat":20,
            "scriptPubKey":{
              "addresses":[
                "address2"
              ]
            }
          }
        ]
      }
    ]
  };


  it('address3 and address4 should be in the same cluster', async () => {
    let c1 = await clusterAddressService.getAddressCluster("address3");
    let c2 = await clusterAddressService.getAddressCluster("address4");
    expect(c1).to.equal(c2);
  });

  it('address3 cluster should contain two addresses', async () => {
    let c1 = await clusterAddressService.getAddressCluster("address3");
    let addresses = await clusterAddressService.getClusterAddresses(c1);
    expect(addresses).lengthOf(2);
  });

  it('address1 cluster should not contain other addresses', async () => {
    let c1 = await clusterAddressService.getAddressCluster("address1");
    let addresses = await clusterAddressService.getClusterAddresses(c1);
    expect(addresses).lengthOf(1);
    expect(addresses).to.include("address1");
  });

  it('address2 cluster balance should be 32 satoshis', async () => {
    let c = await clusterAddressService.getAddressCluster("address2");
    let balance = await clusterBalanceService.getBalance(c);
    expect(balance).to.equal(31);
  });

  it('address1 cluster balance should be 8 satoshis', async () => {
    let c = await clusterAddressService.getAddressCluster("address1");
    let balance = await clusterBalanceService.getBalance(c);
    expect(balance).to.equal(8);
  });

  it('address3 cluster should contain 3 transactions', async () => {
    let c = await clusterAddressService.getAddressCluster("address3");
    let transactions = await clusterBalanceService.getClusterTransactions(c);
    expect(transactions).lengthOf(3);
  });

  it('address3 cluster transaction indexes should be [0, 1, 2]', async () => {
    let c = await clusterAddressService.getAddressCluster("address3");
    let transactions = await clusterBalanceService.getClusterTransactions(c);
    let indexes = transactions.map(tx => tx.id);
    expect(indexes).to.deep.equal([0, 1, 2]);
  });

  it('address3 cluster balance should be 0', async () => {
    let c = await clusterAddressService.getAddressCluster("address3");
    let balance = await clusterBalanceService.getBalance(c);
    expect(balance).to.equal(0);
  });

});