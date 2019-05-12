import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_cluster_address_prefix } from "../misc/db-constants";
import { ClusterId } from '../models/clusterid';
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class ClusterAddressTable extends PrefixTable< { clusterId: ClusterId, balance?: number, address?: string}, { }> {
  
  constructor(db: BinaryDB, private addressEncodingService: AddressEncodingService) {
    super(db);
  }
  
  prefix = db_cluster_address_prefix;
  keyencoding = {
    encode: (key: { clusterId: ClusterId, balance?: number, address?: string}): Buffer => {
      let components: Buffer[] = [];
      components.push(key.clusterId.encode());
      if (key.balance !== undefined) components.push(lexi.encode(key.balance));
      if (key.address !== undefined) components.push(this.addressEncodingService.addressToBytes(key.address));
      return Buffer.concat(components);
    },
    decode: (buf: Buffer): { clusterId: ClusterId, balance: number, address: string} => {
      let offset = 0;
      let clusterId = ClusterId.decode(buf, offset);
      offset += clusterId.byteLength;
      let balance = lexi.decode(buf, offset);
      offset += balance.byteLength;
      let address = this.addressEncodingService.bytesToAddress(buf, offset);
      return {
        clusterId: clusterId.value,
        balance: balance.value,
        address: address
      };
    }
  };

  valueencoding = {
    encode: (key: { }): Buffer => {
      return Buffer.alloc(0);
    },
    decode: (buf: Buffer): { } => {
      return {
      };
    }
  };
}