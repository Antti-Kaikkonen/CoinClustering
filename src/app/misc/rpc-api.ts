import RpcClient from 'bitcoind-rpc';
import { inject, injectable, named } from 'inversify';
import { Transaction } from '../models/transaction';

const rpc_batch_size = 30;

@injectable()
export default class RpcApi {

  private txCache: Map<string, Transaction> = new Map();
  private cacheMaxSize: number = 100000;

  rpc: RpcClient;

  constructor(@inject("string") @named("host") private host: string, 
    @inject("number") @named("port") private port: number, 
    @inject("string") @named("user") user: string, 
    @inject("string") @named("pass") pass: string, 
    @inject("string") @named("protocol") protocol: 'http' | 'https' = "http") {
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
    let cachedTransactions: Transaction[] = [];
    let nonCachedTxids: string[] = [];
    txids.forEach((txid: string, index: number) => {
      let cached = this.txCache.get(txid);
      if (cached) {
        this.txCache.delete(txid);//update recentness in cache
        this.txCache.set(txid, cached);
        cachedTransactions.push(cached);
        console.log("cached");
      } else {
        nonCachedTxids.push(txid);
      }
    });
    let from = 0;
    let promises = [];
    let nonCachedTransactions: Transaction[] = [];
    while (from < nonCachedTxids.length) {
      promises.push(this.getTransactionsHelper(nonCachedTxids.slice(from, from+rpc_batch_size)));//To avoid HTTP 413 error
      if (promises.length > 100) {
        let batches = await Promise.all(promises);
        batches.forEach(txs => txs.forEach(tx =>nonCachedTransactions.push(tx)));
        promises = [];
      }
      from+=rpc_batch_size;
    }
    let batches = await Promise.all(promises);
    batches.forEach(rawtxs => rawtxs.forEach(rawtx =>nonCachedTransactions.push(rawtx)));

    nonCachedTransactions.forEach(tx => {
      this.txCache.set(tx.txid, tx);
      if (this.txCache.size > this.cacheMaxSize) this.txCache.delete(this.txCache.keys().next().value);//remove oldest
    });

    let nonCachedIndex = 0;
    let cachedIndex = 0;
    let result: Transaction[] = [];
    txids.forEach(txid => {
      if (cachedIndex < cachedTransactions.length && cachedTransactions[cachedIndex]) {
        result.push(cachedTransactions[cachedIndex]);
        cachedIndex++;
      } else {
        result.push(nonCachedTransactions[nonCachedIndex]);
        nonCachedIndex++;
      }
    });
    return result;
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