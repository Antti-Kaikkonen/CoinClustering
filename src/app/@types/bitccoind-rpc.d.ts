declare module 'bitcoind-rpc' {

  export interface RpcClientOptions {
    host?: string,
    port?: number,
    user?: string,
    pass?: string,
    protocol?: 'http' | 'https',
    rejectUnauthorized?: boolean
  }

  export interface RpcResult<T> {
    result?: T,
    error?: any,
    id: number
  }

  export default class RpcClient {
    constructor(opts: RpcClientOptions);

    getBlock(hash: string, cb?: (err, res: RpcResult<any>)=>void): void;
    
    decodeRawTransaction(txid: string, cb?: (err, res: RpcResult<any>)=>void): void;

    getRawTransaction(txid: string, verbose?: number, cb?: (err, res: RpcResult<any>)=>void): void;

    batch(batchcall, fn: (err, res: RpcResult<any>[])=>void): void;

    getBlockHash(height: number, fn?: (err, res: RpcResult<string>)=>void): void;

    getBlockCount(cb?: (err, res: RpcResult<number>)=>void): void;
  }
}  
