import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_cluster_balance_prefix } from "../misc/db-constants";
import { ClusterId } from '../models/clusterid';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class ClusterBalanceTable extends PrefixTable< { clusterId: ClusterId}, 
{ balance: number }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_cluster_balance_prefix;

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