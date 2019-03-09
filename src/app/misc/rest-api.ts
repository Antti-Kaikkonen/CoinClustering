import * as http from 'http';
import { inject, injectable, named } from 'inversify';
import "reflect-metadata";
import { BlockWithTransactions } from '../models/block';

@injectable()
export default class RestApi {


  constructor(
    @inject("string") @named("host") private host: string, 
    @inject("number") @named("port") private port: number) {
  }

  async restblock(hash: string): Promise<BlockWithTransactions> {
    return new Promise<BlockWithTransactions>((resolve, reject) => {
      http.get("http://"+this.host+":"+this.port+"/rest/block/"+hash+".json", (resp: http.IncomingMessage) => {
  
        let data = '';
        resp.on('data', (chunk) => {
          data += chunk;
        });
    
        resp.on('end', () => {
          resolve(JSON.parse(data));
        });
      });
    });
  }
}