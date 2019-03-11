import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_cluster_transaction_prefix } from "../misc/db-constants";
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

const TXID_BYTE_LENGTH = 32;
const SIGN_NEGATIVE = 0;

@injectable()
export class ClusterTransactionTable extends PrefixTable< { clusterId: number, height?: number, n?: number}, 
{ txid: string, balanceChange: number }> {

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
    encode: (key: { txid: string, balanceChange: number }): Buffer => {
      let txidBytes = Buffer.from(key.txid, 'hex');
      if (txidBytes.length !== TXID_BYTE_LENGTH) throw Error("TXID must be " + TXID_BYTE_LENGTH +" bytes");
      let balanceChangeBytes = lexi.encode(Math.abs(key.balanceChange));
      let signBytes: Buffer;
      if (key.balanceChange < 0) 
        signBytes = Buffer.from([SIGN_NEGATIVE]); 
      else 
        signBytes = Buffer.alloc(0);
      return Buffer.concat([txidBytes, balanceChangeBytes, signBytes]);
    },
    decode: (buf: Buffer): { txid: string, balanceChange: number } => {
      let offset = 0;
      let txid = buf.toString('hex', offset, TXID_BYTE_LENGTH);
      offset += TXID_BYTE_LENGTH;
      let balanceChange = lexi.decode(buf, offset);
      offset += balanceChange.byteLength;
      if (offset < buf.length && buf[offset] === SIGN_NEGATIVE) {
        balanceChange.value *= -1;
      }
      return {
        txid: txid,
        balanceChange: balanceChange.value
      };
    }
  };
}