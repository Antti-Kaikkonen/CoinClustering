import RpcClient from 'bitcoind-rpc';
import { BinaryDB } from './binary-db';

export class BlockService {

  constructor(private db: BinaryDB, private rpc: RpcClient) {
  }  

  async getRpcBlockHash(height: number): Promise<string> {
    return new Promise<string>((resolve, reject) => {
      this.rpc.getBlockHash(height, (err , res) => {
        resolve(res.result);
      });
    });
  }

}  