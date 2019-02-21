import * as lexi from 'lexint';
import { db_cluster_balance_prefix } from "../misc/db-constants";
import { PrefixTable } from './prefix-table';

export class ClusterBalanceTable extends PrefixTable< { clusterId: number}, 
{ balance: number }> {

  prefix = db_cluster_balance_prefix;

  keyencoding = {
    encode: (key: { clusterId: number}): Buffer => {
      return lexi.encode(key.clusterId);
    },
    decode: (buf: Buffer): { clusterId: number} => {
      return {
        clusterId: lexi.decode(buf).value
      };
    }
  };

  valueencoding = {
    encode: (key: { balance: number }): Buffer => {
      if (!Number.isInteger(key.balance)) throw new Error("Balance must be integer. ("+key.balance+" isn't)");
      if (key.balance < 0) throw new Error("Balance must be positive ("+key.balance+"<"+0+")");
      return lexi.encode(key.balance);
    },
    decode: (buf: Buffer): { balance: number } => {
      return {
        balance: lexi.decode(buf).value
      };
    }
  };

}  