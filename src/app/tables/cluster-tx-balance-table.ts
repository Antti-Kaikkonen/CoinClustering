import * as lexi from 'lexint';
import { db_cluster_tx_balance_prefix } from "../services/db-constants";
import { PrefixTable } from './prefix-table';

export class ClusterTxBalanceTable extends PrefixTable< { clusterId: number, txid?: string }, 
{ transactionIndex: number, balance: number, height: number, n: number }> {

  prefix = db_cluster_tx_balance_prefix;
  keyencoding = {
    encode: (key: { clusterId: number, txid?: string }): Buffer => {
      if (key.txid === undefined) 
        return Buffer.from(key.txid, 'hex');
      else
        return Buffer.concat([Buffer.from(lexi.encode(key.clusterId)), Buffer.from(key.txid, 'hex')]);
    },
    decode: (buf: Buffer): { clusterId: number, txid: string } => {
      let clusterId = lexi.decode(buf, 0);
      let txid = buf.toString('hex', clusterId.byteLength);
      return {
        clusterId: clusterId.value,
        txid: txid
      };
    }
  };

  valueencoding = {
    encode: (key: { transactionIndex: number, balance: number, height: number, n: number }): Buffer => {
      let txidBytes = lexi.encode(key.transactionIndex);
      let balanceBytes = lexi.encode(key.balance);
      let heightBytes = lexi.encode(key.height);
      let nBytes = lexi.encode(key.n);
      return Buffer.concat([txidBytes, balanceBytes, heightBytes, nBytes]);
    },
    decode: (buf: Buffer): { transactionIndex: number, balance: number, height: number, n: number } => {
      let offset = 0;
      let transactionIndex = lexi.decode(buf, offset);
      offset += transactionIndex.byteLength;
      let balance = lexi.decode(buf, offset);
      offset += balance.byteLength;
      let height = lexi.decode(buf, offset);
      offset += height.byteLength;
      let n = lexi.decode(buf, offset);
      return {
        transactionIndex: transactionIndex.value,
        balance: balance.value,
        height: height.value,
        n: n.value
      };
    }
  };
}