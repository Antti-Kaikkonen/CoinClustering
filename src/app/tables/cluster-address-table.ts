import bs58 from 'bs58';
import * as lexi from 'lexint';
import { db_cluster_address_prefix } from "../services/db-constants";
import { PrefixTable } from './prefix-table';

export class ClusterAddressTable extends PrefixTable< { clusterId: number, addressIndex?: number}, { address: string }> {

  prefix = db_cluster_address_prefix;
  keyencoding = {
    encode: (key: { clusterId: number, addressIndex?: number}): Buffer => {
      let res: Buffer;
      if (key.addressIndex === undefined) 
        res=lexi.encode(key.clusterId);
      else
        res=Buffer.concat([lexi.encode(key.clusterId), lexi.encode(key.addressIndex)]);
      //console.log("clusterAddressTable encoding", key, res);
      return res;
    },
    decode: (buf: Buffer): { clusterId: number, addressIndex?: number} => {
      let offset = 0;
      let clusterId = lexi.decode(buf, offset);
      offset += clusterId.byteLength;
      let addressIndex = lexi.decode(buf, offset);
      let res = {
        clusterId: clusterId.value,
        addressIndex: addressIndex.value
      };
      //console.log("clusterAddressTabke decoding", buf, res);
      return res;
    }
  };

  valueencoding = {
    encode: (key: { address: string }): Buffer => {
      return bs58.decode(key.address);
    },
    decode: (buf: Buffer): { address: string } => {
      return {
        address: bs58.encode(buf)
      };
    }
  };
}