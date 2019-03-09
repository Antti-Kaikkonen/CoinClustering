import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_cluster_merged_to } from "../misc/db-constants";
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class ClusterMergedToTable extends PrefixTable< { fromClusterId: number}, { toClusterId: number }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_cluster_merged_to;

  keyencoding = {
    encode: (key: { fromClusterId: number}): Buffer => {
        return Buffer.from(lexi.encode(key.fromClusterId));
    },
    decode: (buf: Buffer): { fromClusterId: number} => {
      let clusterId = lexi.decode(buf, 0);
      return {
        fromClusterId: clusterId.value
      };
    }
  };

  valueencoding = {
    encode: (key: { toClusterId: number }): Buffer => {
      return lexi.encode(key.toClusterId);
    },
    decode: (buf: Buffer): { toClusterId: number } => {
      return {
        toClusterId: lexi.decode(buf).value
      };  
    }
  };
}