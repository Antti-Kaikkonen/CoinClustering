import RpcClient from 'bitcoind-rpc';
import { injectable } from 'inversify';


@injectable()
export class BlockService {

  constructor(private rpc: RpcClient) {
  }  

  async getRpcBlockHash(height: number): Promise<string> {
    return new Promise<string>((resolve, reject) => {
      this.rpc.getBlockHash(height, (err , res) => {
        resolve(res.result);
      });
    });
  }

}  