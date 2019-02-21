import RpcClient from 'bitcoind-rpc';
import { Transaction } from '../models/transaction';

const rpc_batch_size = 30;

export default class RpcApi {

  rpc: RpcClient;

  constructor(private host: string, private port: number, user: string, pass: string, protocol: 'http' | 'https' = "http") {
    this.rpc = new RpcClient({host: host, port: port, protocol: protocol, user: user, pass: pass});
  }

  private async getTransactionsHelper(txids: string[]): Promise<Transaction[]> {
    let batchCall = () => {
      txids.forEach(txid => this.rpc.getRawTransaction(txid, 1));
    }
    return new Promise<Transaction[]>((resolve, reject) => {
      this.rpc.batch(batchCall, (err, rawtxs) => {
        if (err) reject(err)
        else if (rawtxs.length > 0 && rawtxs[0].error) reject(rawtxs[0].error.message)
        else resolve(rawtxs.map(rawtx => rawtx.result));
      });
    });  
  }
  
  async getTransactions(txids: string[]): Promise<Transaction[]> {
    let res: Transaction[] = [];
    let from = 0;
    let promises = [];
    while (from < txids.length) {
      promises.push(this.getTransactionsHelper(txids.slice(from, from+rpc_batch_size)));//To avoid HTTP 413 error
  
      if (promises.length > 100) {
        let batches = await Promise.all(promises);
        batches.forEach(txs => txs.forEach(tx =>res.push(tx)));
        promises = [];
      }
      from+=rpc_batch_size;
    }
    let batches = await Promise.all(promises);
    batches.forEach(rawtxs => rawtxs.forEach(rawtx =>res.push(rawtx)));
    return res;
  }
  
  async getRpcHeight(): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      this.rpc.getBlockCount((err, res) => {
        let height = res.result;
        resolve(height);
      });
    });
  }

  async getRpcBlockHash(height: number): Promise<string> {
    return new Promise<string>((resolve, reject) => {
      this.rpc.getBlockHash(height, (err , res) => {
        resolve(res.result);
      });
    });
  }

}