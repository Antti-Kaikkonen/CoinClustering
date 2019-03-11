import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_cluster_transaction_count_prefix } from '../misc/db-constants';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class ClusterTransactionCountTable extends PrefixTable< { clusterId: number}, { transactionCount: number }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_cluster_transaction_count_prefix;

  keyencoding = {
    encode: (key: { clusterId: number}): Buffer => {
        return lexi.encode(key.clusterId);
    },
    decode: (buf: Buffer): { clusterId: number, addressIndex?: number} => {
      let clusterId = lexi.decode(buf, 0).value;
      return {
        clusterId: clusterId
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