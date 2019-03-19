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
    let mergedHeightPromise: Promise<{height: number}> = this.lastMergedHeightTable.get(undefined)
    let txHeightPromise: Promise<{height: number}> = this.lastSavedTxHeightTable.get(undefined);
    res.json({
      rpcHeight: await rpcHeightPromise,
      clustersProcessedHeight: (await mergedHeightPromise).height,
      balancesProcessedHeight: (await txHeightPromise).height
    });
  }  

}  