import { injectable } from 'inversify';
import { Readable, Writable } from 'stream';
import RestApi from '../misc/rest-api';
import RpcApi from '../misc/rpc-api';
import { BlockTimeTable } from '../tables/block-time-table';
import { BlockImportService } from './block-import-service';


@injectable()
export class BlockTimeService {

  heightToTime: Map<number, number>;

  constructor(private blockTimeTable: BlockTimeTable, private rpcApi: RpcApi, private restApi: RestApi, private blockImportService: BlockImportService) {
    this.heightToTime = new Map();
  }

  async init() {
    console.log("Filling block time cache...");
    await new Promise((resolve, reject) => {
      this.blockTimeTable.createReadStream().on('data', (data) => {
        let height: number = data.key.height;
        let time: number = data.value.time;
        this.heightToTime.set(height, time);
      }).on('end', () => {
        resolve();
      });
    });

    let targetBlock: number = await this.blockImportService.getLastSavedTxHeight();

    let currentHeight = 1;

    let writable = new Writable({
      objectMode: true,
      write: (promise: Promise<any>, encoding, callback) => {
        promise.then((res) => callback());
      }
    });

    var missingHeightTimeReader = new Readable({
      objectMode: true,
      read: (size) => {
        while (currentHeight <= targetBlock) {
          if (!this.heightToTime.has(currentHeight))  {
            let height = currentHeight;
            let promise = new Promise<any>(async (resolve, reject) => {
              let blockHash = await this.rpcApi.getRpcBlockHash(height);
              let blocks = await this.restApi.blockHeaders(1, blockHash);
              let time = blocks[0].time;
              this.heightToTime.set(height, time);
              await this.blockTimeTable.put({height: height}, {time: time});
              resolve(time);
            });
            missingHeightTimeReader.push(promise);
            currentHeight++;
            return;
          }
          currentHeight++;
        }
        missingHeightTimeReader.push(null);
      }
    });

    await new Promise((resolve, reject) => {
      writable.on('finish', () => {
        resolve();
      });  
      missingHeightTimeReader.pipe(writable);
    });
    console.log("Filling block time cache done");

  }

  setTime(height: number, time: number) {
    this.heightToTime.set(height, time);
    this.blockTimeTable.put({height: height}, {time: time});
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
