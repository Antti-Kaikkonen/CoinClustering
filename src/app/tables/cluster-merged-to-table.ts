import { injectable } from 'inversify';
import { db_cluster_merged_to } from "../misc/db-constants";
import { ClusterId } from '../models/clusterid';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class ClusterMergedToTable extends PrefixTable< { fromClusterId: ClusterId}, { toClusterId: ClusterId }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_cluster_merged_to;

  keyencoding = {
    encode: (key: { fromClusterId: ClusterId}): Buffer => {
        return key.fromClusterId.encode();
    },
    decode: (buf: Buffer): { fromClusterId: ClusterId} => {
      return {
        fromClusterId: ClusterId.decode(buf).value
      };
    }
  };

  valueencoding = {
    encode: (key: { toClusterId: ClusterId }): Buffer => {
      return key.toClusterId.encode();
    },
    decode: (buf: Buffer): { toClusterId: ClusterId } => {
      return {
        toClusterId: ClusterId.decode(buf).value
      };
    }
  };
}