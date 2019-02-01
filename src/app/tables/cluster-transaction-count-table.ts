import * as lexi from 'lexint';
import { db_cluster_transaction_count_prefix } from "../services/db-constants";
import { PrefixTable } from './prefix-table';

export class ClusterTransactionCountTable extends PrefixTable< { clusterId: number}, { balanceCount: number }> {

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
    encode: (key: { balanceCount: number }): Buffer => {
      return lexi.encode(key.balanceCount);
    },
    decode: (buf: Buffer): { balanceCount: number } => {
      return {
        balanceCount: lexi.decode(buf).value
      };  
    }
  };
}