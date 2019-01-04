import bs58 from 'bs58';
import * as lexi from 'lexint';
import { db_address_cluster_prefix } from "../services/db-constants";
import { PrefixTable } from './prefix-table';

export class AddressClusterTable extends PrefixTable< { address: string}, { clusterId: number }> {

  prefix = db_address_cluster_prefix;

  keyencoding = {
    encode: (key: { address: string}): Buffer => {
        return bs58.decode(key.address);
    },
    decode: (buf: Buffer): { address: string } => {
      return {
        address: bs58.encode(buf)
      }
    }
  };

  valueencoding = {
    encode: (key: { clusterId: number }): Buffer => {
      //console.log("addressClusterTable encode value", key, lexi.encode(key.clusterId));
      return lexi.encode(key.clusterId);
    },
    decode: (buf: Buffer): { clusterId: number } => {
      //console.log("addressClusterTable decode value", buf, lexi.decode(buf).value, lexi.decode(buf).byteLength);
      return {
        clusterId: lexi.decode(buf).value
      };
    }
  };
}