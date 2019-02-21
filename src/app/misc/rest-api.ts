import http from 'http';
import { BlockWithTransactions } from '../models/block';

export default class RestApi {


  constructor(private host: string, private port: number) {
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