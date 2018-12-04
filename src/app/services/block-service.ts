import { LevelUp } from 'levelup';
import { integer2LexString } from '../utils/utils';
import { db_block_hash } from './db-constants';

export class BlockService {

  constructor(private db: LevelUp, rpc: any) {

  }  

  async saveBlockHash(height: number, hash: string) {
    return this.db.put(db_block_hash+height, hash);
  }

  async getBlockHash(height: number): Promise<string> {
    return this.db.get(db_block_hash+height);
  }

  async getLastChainBlock() {
    //TODO
  }

  async getLastBlock(): Promise<{height: number, hash: string}> {
    return new Promise<{height: number, hash: string}>((resolve, reject) => {
      this.db.createReadStream({
        gte:db_block_hash+integer2LexString(0),
        lt:db_block_hash+"/z",
        reverse: true,
        limit: 1
      })
      .on('data', function (data) {
        let key: string = data.key;
        let height: number = Number(key.substr(db_block_hash.length));
        resolve({height: height, hash: data.value});
      })
      .on('error', function (err) {
        reject(err);
      })
      .on('close', function () {
      })
      .on('end', function () {
      });
    });
  }

}  