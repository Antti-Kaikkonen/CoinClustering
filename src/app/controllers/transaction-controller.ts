import { Request, Response } from "express";
import RpcApi from "../misc/rpc-api";
import { txAddressBalanceChanges } from "../misc/utils";
import { Transaction } from "../models/transaction";
import { AddressEncodingService } from "../services/address-encoding-service";
import { BinaryDB } from "../services/binary-db";
import { ClusterAddressService } from "../services/cluster-address-service";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { OutputCacheTable } from "../tables/output-cache-table";

export class TransactionController {


  private addressClusterTable: AddressClusterTable;
  private outputCacheTable: OutputCacheTable;
  private clusterAddressService: ClusterAddressService;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService, private rpcApi: RpcApi) {
    this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
    this.outputCacheTable = new OutputCacheTable(this.db, addressEncodingService);
    this.clusterAddressService = new ClusterAddressService(db, addressEncodingService);
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