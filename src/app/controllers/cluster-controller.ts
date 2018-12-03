import { Request, Response } from 'express';

import { ClusterAddressService } from '../services/cluster-address-service';
import { ClusterBalanceService } from '../services/cluster-balance-service';

export class ClusterController {

  constructor(private clusterBalanceService: ClusterBalanceService, private clusterAddressService: ClusterAddressService) {

  }  

  clusterCurrentBalances = async (req: Request, res: Response) => {
    let from: number = Number(req.query.from);
    if (isNaN(from)) from = 0;
    let to: number = Number(req.query.to);
    if (isNaN(to)) to = from+10;
    let promises = [];
    for (let i = from; i < to; i++) {
      promises.push(this.clusterBalanceService.getLast(""+i));
    }
    let values = await Promise.all(promises);
    values = values.map((v, index:number) => { 
      return {clusterId: index+from, b: v}
    });
    res.send(values);
  };

  clusterTransactions = async (req:Request, res:Response) => {
    let clusterId: number = req.query.id || 0;
    let result = await this.clusterBalanceService.getClusterTransactions(""+clusterId);
    res.send(result);
  };

  clusterAddresses = async (req:Request, res:Response) => {
    let clusterId: number = Number(req.params.id);
    let result = await this.clusterAddressService.getClusterAddresses(""+clusterId);
    res.send(result);
  };

}  