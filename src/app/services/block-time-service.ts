import { injectable } from 'inversify';
import RestApi from '../misc/rest-api';
import RpcApi from '../misc/rpc-api';
import { BlockTimeTable } from '../tables/block-time-table';


@injectable()
export class BlockTimeService {

  heightToTime: Map<number, number> = new Map();

  constructor(private blockTimeTable: BlockTimeTable, private rpcApi: RpcApi, private restApi: RestApi) {
  }  

  async getTime(height: number): Promise<number> {
    let cached = this.heightToTime.get(height);
    if (cached !== undefined) return cached;
    let time: number;
    try {
      time = (await this.blockTimeTable.get({height: height})).time;
    } catch (err) {
      if (err.notFound) {
        let blockHash = await this.rpcApi.getRpcBlockHash(height);
        let blocks = await this.restApi.blockHeaders(1, blockHash);
        time = blocks[0].time;
        this.blockTimeTable.put({height: height}, {time: time});
      } else throw err;
    }
    this.heightToTime.set(height, time);
    return time;
  }

}  
