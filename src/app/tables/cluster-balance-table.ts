import * as lexi from 'lexint';
import { db_cluster_balance_prefix } from "../services/db-constants";
import { PrefixTable } from './prefix-table';

const TXID_BYTE_LENGTH = 32;

export class ClusterBalanceTable extends PrefixTable< { clusterId: number, transactionIndex?: number}, 
{ txid: string, balance: number, height: number, n: number }> {

  prefix = db_cluster_balance_prefix;
  keyencoding = {
    encode: (key: { clusterId: number, transactionIndex?: number}): Buffer => {
      //console.log("encoding ", key);
      if (key.transactionIndex === undefined) 
        return Buffer.from(lexi.encode(key.clusterId));
      else
        return Buffer.concat([Buffer.from(lexi.encode(key.clusterId)), lexi.encode(key.transactionIndex)]);
    },
    decode: (buf: Buffer): { clusterId: number, transactionIndex?: number} => {
      let clusterId = lexi.decode(buf, 0);
      let transactionIndex = lexi.decode(buf, clusterId.byteLength);
      return {
        clusterId: clusterId.value,
        transactionIndex: transactionIndex.value
      };
    }
  };

  valueencoding = {
    encode: (key: { txid: string, balance: number, height: number, n: number }): Buffer => {
      key.balance++;
      if (key.balance < 0) throw Error("Balance must be non negative");
      if (!Number.isInteger(key.balance)) throw Error("Balance must be an integer");
      let txidBytes = Buffer.from(key.txid, 'hex');
      if (txidBytes.length !== TXID_BYTE_LENGTH) throw Error("TXID must be " + TXID_BYTE_LENGTH +" bytes");
      let balanceBytes = lexi.encode(key.balance);
      let heightBytes = lexi.encode(key.height);
      let nBytes = lexi.encode(key.n);
      //console.log("cluster-balance-table valueencoding", key, Buffer.concat([txidBytes, balanceBytes, heightBytes, nBytes]));
      return Buffer.concat([txidBytes, balanceBytes, heightBytes, nBytes]);
    },
    decode: (buf: Buffer): { txid: string, balance: number, height: number, n: number } => {
      let offset = 0;
      let txid = buf.toString('hex', offset, 32);
      offset += 32;
      let balance = lexi.decode(buf, offset);
      balance.value--;
      offset += balance.byteLength;
      let height = lexi.decode(buf, offset);
      offset += height.byteLength;
      let n = lexi.decode(buf, offset);
      /*console.log("cluster-balance-table valuedecoding", buf, {
        txid: txid,
        balance: balance.value,
        height: height.value,
        n: n.value
      });*/
      return {
        txid: txid,
        balance: balance.value,
        height: height.value,
        n: n.value
      };
    }
  };
}