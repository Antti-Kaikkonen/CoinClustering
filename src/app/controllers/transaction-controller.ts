import { Request, Response } from "express";
import RpcApi from "../misc/rpc-api";
import { txAddressBalanceChanges } from "../misc/utils";
import { Transaction } from "../models/transaction";
import { AddressEncodingService } from "../services/address-encoding-service";
import { BinaryDB } from "../services/binary-db";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { OutputCacheTable } from "../tables/output-cache-table";

export class TransactionController {


  private addressClusterTable: AddressClusterTable;
  private outputCacheTable: OutputCacheTable;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService, private rpcApi: RpcApi) {
    this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
    this.outputCacheTable = new OutputCacheTable(this.db, addressEncodingService);
  }  

  private async addressBalanceChangesToClusterBalanceChanges(addressToDelta: Map<string, number>): Promise<Map<number, number>> {
    let promises = [];
    let addresses = [];
    addressToDelta.forEach((delta: number, address: string) => {
      addresses.push(address);
      promises.push(this.addressClusterTable.get({address: address}));
    });
    let clusterIds = await Promise.all(promises);
    let clusterToDelta = new Map<number, number>();
    addresses.forEach((address: string, index: number) => {
      let clusterId: number = clusterIds[index].clusterId;
      if (clusterId === undefined) throw Error("Cluster missing");
      let oldBalance = clusterToDelta.get(clusterId);
      let addressDelta = addressToDelta.get(address);
      if (!oldBalance) oldBalance = 0;
      clusterToDelta.set(clusterId, oldBalance+addressDelta);
    });
    return clusterToDelta;
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
    let clusterBalanceChanges = await this.addressBalanceChangesToClusterBalanceChanges(balanceChanges);
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