# CoinClustering
Address clustering for bitcoin based cryptocurrencies

Under construction!

bitcoin.conf / dash.conf / ... required settings
server=1
txindex=1
rest=1

## HTTP API

### Clusters
<details>
 <summary>GET /clusters</summary>

# Query parameters
* after (optional)
* limit (optional)
# Example
## Request
`/clusters?limit=5`
## Response
``` json
[
  {
    "clusterId": 34737550,
    "balance": 116069306126068
  },
  {
    "clusterId": 3406070,
    "balance": 99398189164815
  },
  {
    "clusterId": 15450407,
    "balance": 89713539855629
  },
  {
    "clusterId": 15419485,
    "balance": 89707097731123
  },
  {
    "clusterId": 15449624,
    "balance": 89552106923194
  }
]
```
</details>
<details>
 <summary>GET /clusters/:id/summary</summary>

# Example
## Request
`/clusters/867498/summary`
## Response
``` json
{
  "balance": 0,
  "firstTransaction": {
    "txid": "31f3710a0a6fede8d198fd1da26124dcc55d3bb4afad818050aa2413dc210564",
    "height": 313604,
    "n": 1
  },
  "lastTransaction": {
    "txid": "2608555083f4e2b46189f395fee7c4e44fe462f3da0faa07e3ad3af10ebc98d7",
    "height": 313652,
    "n": 1
  },
  "addressCount": 3
}
```
</details>
<details>
 <summary>GET /clusters/:id/transactions</summary>

# Query parameters
* after (optional)
* limit (optional)
# Example
## Request
`/clusters/867498/transactions`
## Response
``` json
[
  {
    "txid": "31f3710a0a6fede8d198fd1da26124dcc55d3bb4afad818050aa2413dc210564",
    "height": 313604,
    "n": 1
  },
  {
    "txid": "9d05dc6580b7336a01ba83ee8adbfb214950dc42252ae4fdca45424cbc7519e2",
    "height": 313613,
    "n": 1
  },
  {
    "txid": "d0414c14591e307157b72a7091dd46be4763d1ad3d2edcb00547f43b5fe6b4b8",
    "height": 313626,
    "n": 1
  },
  {
    "txid": "2608555083f4e2b46189f395fee7c4e44fe462f3da0faa07e3ad3af10ebc98d7",
    "height": 313652,
    "n": 1
  }
]
```
</details>
<details>
 <summary>GET /clusters/:id/addresses</summary>

# Query parameters
* after (optional)
* limit (optional)
# Example
## Request
`/clusters/867498/addresses`
## Response
``` json
[
  {
    "addressIndex": 0,
    "address": "Lg1a7xRpiyMVAKvhLCrh78TEg1SLrVt2Eg"
  },
  {
    "addressIndex": 1,
    "address": "LZvdRoB5LWPdodp8qsWLudgYZ4cFQ9syCx"
  },
  {
    "addressIndex": 2,
    "address": "LXQUw5bQvuGFjf9uMR3dey8zLK8v2NkW8N"
  }  
]
```
</details>

### Addresses
<details>
 <summary>GET /addresses/:address/cluster_id</summary>

`/addresses/Lg1a7xRpiyMVAKvhLCrh78TEg1SLrVt2Eg/cluster_id`
``` json
867498
```
</details>