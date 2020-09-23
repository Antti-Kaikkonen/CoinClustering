## CoinClustering
Address clustering for bitcoin based cryptocurrencies

Under construction!

## Prerequisites

Node and NPM

[Bitcoin core](https://github.com/bitcoin/bitcoin) or API compatible client such as [Litecoin core](https://github.com/litecoin-project/litecoin) or [Dash core](https://github.com/dashpay/dash/)

bitcoin.conf / dash.conf / litecoin.conf...
```
server=1
txindex=1
rest=1
rpcuser=[your username]
rpcpassword=[your password]
```

## Installation

1. `npm install -g coin-clustering`
2. Create a new directory, for example 'BitcoinClustering'
3. Navigate to the directory and create a new file "config.js"
4. Edit config.js according to the below example. Altcoin excamples can be found [here](TODO). 
``` 
let config = {
  protocol: 'http',
  user: '[rpcuser from bitcoin.conf]',
  pass: '[rpcpassword from bitcoin.conf]',
  host: '127.0.0.1',
  port: 8332,
  listen_port: 3006,
  pubkeyhash: 0x00,
  scripthash: 0x05,
  segwitprefix: "bc",
  dbcache: 3000
};

module.exports = config; 
```
5. run `coin-clustering` in the same directory with config.js

Indexing the bitcoin blockchain can take more or less than a week depending on your hardware. Most altcoins should take much shorter.

## HTTP API

### Clusters
<details>
 <summary>GET /clusters</summary>

## Query parameters
* gt, gte, lt, lte (optional) = balanceSats / balanceSats-clusterId
* reverse (optional) = true/false, defalt: false
* limit (optional) = integer (0...1000), defalt: 100
## Example
### Request
`/clusters?limit=3&reverse=true&lte=10000000000000`
### Response
**Documentation outdated:** clusterId is now in the form {height, txN, outputN} that refers to first transaction output belonging to a cluster
``` json
[
  {
    "clusterId": 17034506,
    "balance": 9816291286270
  },
  {
    "clusterId": 388710763,
    "balance": 8594734769541
  },
  {
    "clusterId": 421208391,
    "balance": 8366430196393
  }
]
```
</details>
<details>
 <summary>GET /clusters/[height-txN-outputN]/summary</summary>

## Example
### Request
`/clusters/1-2-3/summary`
### Response
``` json
{
  "balance": 9816291286270,
  "firstTransaction": {
    "txid": "2cdce8e3758f9a94975c0b3e1c55729312980cffa0471acfce4d2d308a16b381",
    "height": 256893,
    "n": 4
  },
  "lastTransaction": {
    "txid": "76d4119daa59769f4d694cca9feb1123ebf026e3640ed4ba438c044c809285d8",
    "height": 565263,
    "n": 232
  },
  "addressCount": 114458
}
```
</details>
<details>
 <summary>GET /clusters/[height-txN-outputN]/transactions</summary>

## Query parameters
* gt, gte, lt, lte (optional)
* reverse (optional) = true/false, defalt: false
* limit (optional) = integer (0...1000), defalt: 100
* include-delta (optional) = true/false, defalt: false
## Example
### Request
`/clusters/1-2-3/transactions?limit=3&reverse=true&include-delta=true`
### Response
``` json
[
  {
    "txid": "76d4119daa59769f4d694cca9feb1123ebf026e3640ed4ba438c044c809285d8",
    "height": 565263,
    "n": 232,
    "delta": 95712
  },
  {
    "txid": "55ceaaf9ed5ebf96abed887223a8044afa93119043038321a3d17b982c4337ce",
    "height": 565262,
    "n": 967,
    "delta": 97234
  },
  {
    "txid": "a3f9982bb76d42cc8f68afd12a9a11c6122a30e71ee57cdbd52af767069755e9",
    "height": 563671,
    "n": 571,
    "delta": 615000
  }
]
```
</details>
<details>
 <summary>GET /clusters/[height-txN-outputN]/addresses</summary>

## Query parameters
* gt, gte, lt, lte (optional)
* reverse (optional) = true/false, defalt: false
* limit (optional) = integer (0...1000), defalt: 100
## Example
### Request
`/clusters/1-2-3/addresses?limit=3&reverse=true`
### Response
``` json
[
  {
    "balance": 2322761515776,
    "address": "1AnwDVbwsLBVwRfqN2x9Eo4YEJSPXo2cwG"
  },
  {
    "balance": 2221116525338,
    "address": "14eQD1QQb8QFVG8YFwGz7skyzsvBLWLwJS"
  },
  {
    "balance": 970712966598,
    "address": "1Kd6zLb9iAjcrgq8HzWnoWNVLYYWjp3swA"
  }
]
```
</details>

<details>
 <summary>GET /clusters/[height-txN-outputN]/balance-candlesticks</summary>
</details> 

### Addresses
<details>
 <summary>GET /addresses/[address]/cluster_id</summary>

`/addresses/1AnwDVbwsLBVwRfqN2x9Eo4YEJSPXo2cwG/cluster_id`

**Documentation outdated:** clusterId is now in the form {height, txN, outputN} that refers to first transaction output belonging to a cluster
``` json
17034506
```
</details>
<details>
 <summary>GET /addresses/[address]/transactions</summary>

 ### Query parameters
  * gt, gte, lt, lte (optional)
* reverse (optional) = true/false, defalt: false
* limit (optional) = integer (0...1000), defalt: 100
</details>

<details>
 <summary>GET /addresses/[address]/balance-candlesticks</summary>
</details> 

### Transactions
<details>
 <summary>GET /transactions/[txid]/cluster-balance-changes</summary>
</details>


<details>
 <summary>GET /transactions/[txid]/details</summary>
</details>

### Miscellaneous
<details>
 <summary>GET /status</summary>
</details>

## Limitations
Stays 10 blocks behind the latest block to avoid having to deal with blockchain reorganizations.
