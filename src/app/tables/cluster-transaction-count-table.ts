import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_cluster_transaction_count_prefix } from '../misc/db-constants';
import { ClusterId } from '../models/clusterid';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class ClusterTransactionCountTable extends PrefixTable< { clusterId: ClusterId}, { transactionCount: number }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_cluster_transaction_count_prefix;

  keyencoding = {
    encode: (key: { clusterId: ClusterId}): Buffer => {
        return key.clusterId.encode();
    },
    decode: (buf: Buffer): { clusterId: ClusterId} => {
      return {
        clusterId: ClusterId.decode(buf).value
      };
    }
  };

  valueencoding = {
    encode: (key: { transactionCount: number }): Buffer => {
      return lexi.encode(key.transactionCount);
    },
    decode: (buf: Buffer): { transactionCount: number } => {
      return {
        transactionCount: lexi.decode(buf).value
      };  
    }
  };
}