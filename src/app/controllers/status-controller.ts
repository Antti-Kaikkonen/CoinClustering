import { Request, Response } from 'express';
import { injectable } from 'inversify';
import RpcApi from '../misc/rpc-api';
import { LastMergedHeightTable } from '../tables/last-merged-height-table';
import { LastSavedTxHeightTable } from '../tables/last-saved-tx-height-table';

@injectable()
export class StatusController {

  constructor(
    private lastMergedHeightTable: LastMergedHeightTable,
    private lastSavedTxHeightTable: LastSavedTxHeightTable,
    private rpcApi: RpcApi) {
  }  

  getStatus = async (req: Request, res: Response) => {
    let rpcHeightPromise: Promise<number> = this.rpcApi.getRpcHeight();
    let clustersProcessedHeightPromise: Promise<{height: number}> = this.lastMergedHeightTable.get(undefined)
    let balancesProcessedHeightPromise: Promise<{height: number}> = this.lastSavedTxHeightTable.get(undefined);
    let clustersProcessedHeight: number;
    try{
      clustersProcessedHeight = (await clustersProcessedHeightPromise).height;
    } catch(err) {
      if (err.notFound) clustersProcessedHeight = 0;
    } 
    let balancesProcessedHeight: number;
    try{
      balancesProcessedHeight = (await balancesProcessedHeightPromise).height;
    } catch(err) {
      if (err.notFound) balancesProcessedHeight = 0;
    } 
    res.json({
      rpcHeight: await rpcHeightPromise,
      clustersProcessedHeight: clustersProcessedHeight,
      balancesProcessedHeight: balancesProcessedHeight
    });
  }  

}  