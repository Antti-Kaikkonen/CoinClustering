import { Request, Response } from "express";
import { injectable } from "inversify";
import RpcApi from "../misc/rpc-api";
import { txAddressBalanceChanges, txAddresses } from "../misc/utils";
import { Transaction } from "../models/transaction";
import { ClusterAddressService } from "../services/cluster-address-service";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { OutputCacheTable } from "../tables/output-cache-table";

@injectable()
export class TransactionController {

  constructor(private rpcApi: RpcApi,
    private outputCacheTable: OutputCacheTable,
    private clusterAddressService: ClusterAddressService,
    private addressClusterTable: AddressClusterTable
  ) {
  }  


  txDetailed = async (req:Request, res:Response) => {
    let txid: string = req.params.txid;
    let tx: Transaction = (await this.rpcApi.getTransactions([txid]))[0];
    let attachInputsPromise = new Promise((resolve, reject) => {
      let inputsToAttach = tx.vin.length;
      if (inputsToAttach === 0) resolve();
      tx.vin.forEach(vin => {
        if (!vin.coinbase && !vin.address) {
          this.outputCacheTable.get({txid: vin.txid, n: vin.vout}).then((value) => {
            if (value.addresses.length === 1) {
              vin.address = value.addresses[0];
              vin.value = value.valueSat/1e8;
            }
            inputsToAttach--;
            if (inputsToAttach === 0) resolve();
          })
        } else {
          inputsToAttach--;
          if (inputsToAttach === 0) resolve();
        }
      });
    }); 
    await attachInputsPromise;
    let txAddr = Array.from(txAddresses(tx));
    let addressToClusterId: Map<string, number> = new Map();
    let clusterIdPromises = [];
    txAddr.forEach(address => {
      clusterIdPromises.push(this.addressClusterTable.get({address: address}));
    });
    let clusterIds = await Promise.all(clusterIdPromises);
    txAddr.forEach((address, index) => {
      let clusterId = clusterIds[index].clusterId;
      addressToClusterId.set(address, clusterId);
    });
    tx.vin.forEach(vin => {
      if (vin.address) {
        vin['clusterId'] = addressToClusterId.get(vin.address);
      }
    });
    tx.vout.forEach(vout => {
      if (vout.scriptPubKey && vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1) {
        vout['clusterId'] = addressToClusterId.get(vout.scriptPubKey.addresses[0]);
      }
    });
    delete tx['hex'];
    res.json(tx);
  }  

  txClusterBalnaceChanges = async (req:Request, res:Response) => {
    let txid: string = req.params.txid;
    let tx: Transaction = (await this.rpcApi.getTransactions([txid]))[0];
    let attachInputsPromise = new Promise((resolve, reject) => {
      let inputsToAttach = tx.vin.length;
      if (inputsToAttach === 0) resolve();
      tx.vin.forEach(vin => {
        if (!vin.coinbase && !vin.address) {
          this.outputCacheTable.get({txid: vin.txid, n: vin.vout}).then((value) => {
            if (value.addresses.length === 1) {
              vin.address = value.addresses[0];
              vin.value = value.valueSat/1e8;
            }
            inputsToAttach--;
            if (inputsToAttach === 0) resolve();
          })
        } else {
          inputsToAttach--;
          if (inputsToAttach === 0) resolve();
        }
      });
    }); 
    await attachInputsPromise;
    let balanceChanges: Map<string, number> = txAddressBalanceChanges(tx);
    let clusterBalanceChanges = await this.clusterAddressService.addressBalanceChangesToClusterBalanceChanges(balanceChanges);
    let result = [];
    clusterBalanceChanges.forEach((delta: number, clusterId: number) => {
      result.push({
        clusterId: clusterId,
        delta: delta
      })
    });
    res.send(result);
  }  


}  