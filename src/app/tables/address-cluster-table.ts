import { injectable } from 'inversify';
import { db_address_cluster_prefix } from "../misc/db-constants";
import { ClusterId } from '../models/clusterid';
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class AddressClusterTable extends PrefixTable< { address: string}, { clusterId: ClusterId }> {

  constructor(db: BinaryDB, private addressEncodingService: AddressEncodingService) {
    super(db);
  }

  prefix = db_address_cluster_prefix;

  keyencoding = {
    encode: (key: { address: string}): Buffer => {
      return this.addressEncodingService.addressToBytes(key.address);
        //return bs58.decode(key.address);
    },
    decode: (buf: Buffer): { address: string } => {
      return {
        address: this.addressEncodingService.bytesToAddress(buf)
        //address: bs58.encode(buf)
      }
    }
  };

  valueencoding = {
    encode: (key: { clusterId: ClusterId }): Buffer => {
      //console.log("addressClusterTable encode value", key, lexi.encode(key.clusterId));
      return key.clusterId.encode();
    },
    decode: (buf: Buffer): { clusterId: ClusterId } => {
      //console.log("addressClusterTable decode value", buf, lexi.decode(buf).value, lexi.decode(buf).byteLength);
      return {
        clusterId: ClusterId.decode(buf).value
      };
    }
  };
}