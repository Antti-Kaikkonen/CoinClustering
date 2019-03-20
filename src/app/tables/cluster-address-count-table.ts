import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_cluster_address_count_prefix } from '../misc/db-constants';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class ClusterAddressCountTable extends PrefixTable< { clusterId: number}, { addressCount: number }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_cluster_address_count_prefix;

  keyencoding = {
    encode: (key: { clusterId: number}): Buffer => {
        return lexi.encode(key.clusterId);
    },
    decode: (buf: Buffer): { clusterId: number} => {
      let clusterId = lexi.decode(buf).value;
      return {
        clusterId: clusterId
      };
    }
  };

  valueencoding = {
    encode: (key: { addressCount: number }): Buffer => {
      return lexi.encode(key.addressCount);
    },
    decode: (buf: Buffer): { addressCount: number } => {
      return {
        addressCount: lexi.decode(buf).value
      };  
    }
  };
}