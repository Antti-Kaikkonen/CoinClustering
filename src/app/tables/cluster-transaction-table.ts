import * as lexi from 'lexint';
import { db_cluster_transaction_prefix } from "../services/db-constants";
import { PrefixTable } from './prefix-table';

const TXID_BYTE_LENGTH = 32;
const SIGN_NEGATIVE = 0;

export class ClusterTransactionTable extends PrefixTable< { clusterId: number, height?: number, n?: number}, 
{ txid: string, balanceDelta: number }> {

  prefix = db_cluster_transaction_prefix;
  keyencoding = {
    encode: (key: { clusterId: number, height?: number, n?: number}): Buffer => {
      //console.log("encoding ", key);
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
    encode: (key: { txid: string, balanceDelta: number }): Buffer => {
      if (!Number.isInteger(key.balanceDelta)) throw Error("balanceDelta ("+key.balanceDelta+") must be an integer");
      let txidBytes = Buffer.from(key.txid, 'hex');
      if (txidBytes.length !== TXID_BYTE_LENGTH) throw Error("TXID must be " + TXID_BYTE_LENGTH +" bytes");
      let balanceBytes = lexi.encode(Math.abs(key.balanceDelta));
      let signBytes: Buffer;
      if (key.balanceDelta < 0) 
        signBytes = Buffer.from([SIGN_NEGATIVE]); 
      else 
        signBytes = Buffer.alloc(0);
      //console.log("cluster-balance-table valueencoding", key, Buffer.concat([txidBytes, balanceBytes, heightBytes, nBytes]));
      return Buffer.concat([txidBytes, balanceBytes, signBytes]);
    },
    decode: (buf: Buffer): { txid: string, balanceDelta: number } => {
      let offset = 0;
      let txid = buf.toString('hex', offset, 32);
      offset += 32;
      let balanceDelta = lexi.decode(buf, offset);
      if (Number.isNaN(balanceDelta.value)) {
        console.log("BALANCEDELTA NAN", balanceDelta.value, balanceDelta.byteLength);
      }
      offset += balanceDelta.byteLength;
      if (offset < buf.length && buf[offset] === SIGN_NEGATIVE) {
        balanceDelta.value *= -1;
      }
      return {
        txid: txid,
        balanceDelta: balanceDelta.value
      };
    }
  };
}