import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_cluster_transaction_prefix } from "../misc/db-constants";
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

const TXID_BYTE_LENGTH = 32;
const SIGN_NEGATIVE = 0;

@injectable()
export class ClusterTransactionTable extends PrefixTable< { clusterId: number, height?: number, n?: number}, 
{ txid: string }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_cluster_transaction_prefix;
  keyencoding = {
    encode: (key: { clusterId: number, height?: number, n?: number}): Buffer => {
      if (key.height === undefined) 
        return lexi.encode(key.clusterId);
      else if (key.n === undefined)
        return Buffer.concat([lexi.encode(key.clusterId), lexi.encode(key.height)]);
      else 
        return Buffer.concat([lexi.encode(key.clusterId), lexi.encode(key.height), lexi.encode(key.n)]);
    },
    decode: (buf: Buffer): { clusterId: number, height: number, n: number} => {
      let clusterId = lexi.decode(buf, 0);
      let height = lexi.decode(buf, clusterId.byteLength);
      let n = lexi.decode(buf, clusterId.byteLength+height.byteLength);
      return {
        clusterId: clusterId.value,
        height: height.value,
        n: n.value
      };
    }
  };

  valueencoding = {
    encode: (key: { txid: string }): Buffer => {
      let txidBytes = Buffer.from(key.txid, 'hex');
      if (txidBytes.length !== TXID_BYTE_LENGTH) throw Error("TXID must be " + TXID_BYTE_LENGTH +" bytes");
      return txidBytes;
    },
    decode: (buf: Buffer): { txid: string } => {
      let offset = 0;
      let txid = buf.toString('hex', offset, 32);
      return {
        txid: txid
      };
    }
  };
}