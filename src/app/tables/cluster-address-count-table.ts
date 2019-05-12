import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_cluster_address_count_prefix } from '../misc/db-constants';
import { ClusterId } from '../models/clusterid';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class ClusterAddressCountTable extends PrefixTable< { clusterId: ClusterId }, { addressCount: number }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_cluster_address_count_prefix;

  keyencoding = {
    encode: (key: { clusterId: ClusterId}): Buffer => {
        return key.clusterId.encode();
    },
    decode: (buf: Buffer): { clusterId: ClusterId} => {
      return {
        clusterId: ClusterId.decode(buf, 0).value
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