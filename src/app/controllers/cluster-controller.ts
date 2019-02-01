import { Request, Response } from 'express';
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { ClusterAddressService } from '../services/cluster-address-service';
import { ClusterTransactionService } from '../services/cluster-transaction-service';
import { BalanceToClusterTable } from '../tables/balance-to-cluster-table';


export class ClusterController {

  private clusterTransactionService: ClusterTransactionService;
  private clusterAddressService: ClusterAddressService;
  private balanceToClusterTable: BalanceToClusterTable;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService) {
    this.clusterTransactionService = new ClusterTransactionService(db);
    this.clusterAddressService = new ClusterAddressService(db, addressEncodingService);
    this.balanceToClusterTable = new BalanceToClusterTable(db);
  }  

  clusterTransactions = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);
    let result = await this.clusterTransactionService.getClusterTransactions(clusterId);
    res.send(result);
  };

  clusterAddresses = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);
    let result = await this.clusterAddressService.getClusterAddresses(clusterId);
    res.send(result);
  };


  largestClusters = async (req:Request, res:Response) => {
    res.contentType('application/json');
    let n: number = Number(req.params.n);
    let result = [];
    res.write('[');
    let first = true;
    let stream = this.balanceToClusterTable.createReadStream({reverse: true, limit: n})
    .on('data', (data) => {
      if (!first) res.write(",");
      res.write(JSON.stringify({
        clusterId: data.key.clusterId,
        balance: data.key.balance
      }));
      first = false;
    }).on('finish', () => {
      res.write(']');
      res.end();
    });
    req.on('close', () => {
      console.log("cancelled by user. destroying");
      stream['destroy']();
      console.log("destroyed");
    });
  }

}  