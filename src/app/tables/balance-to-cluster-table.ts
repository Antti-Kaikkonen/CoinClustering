import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_balace_to_cluster_prefix } from "../misc/db-constants";
import { ClusterId } from '../models/clusterid';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class BalanceToClusterTable extends PrefixTable< { balance: number, clusterId?: ClusterId}, 
{  }> {


  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_balace_to_cluster_prefix;
  keyencoding = {
    encode: (key: { balance: number, clusterId: ClusterId}): Buffer => {
      //console.log("encoding ", key);
      if (key.clusterId === undefined) 
        return Buffer.from(lexi.encode(key.balance));
      else
        return Buffer.concat([Buffer.from(lexi.encode(key.balance)), key.clusterId.encode()]);
    },
    decode: (buf: Buffer): { balance: number, clusterId: ClusterId} => {
      let balance = lexi.decode(buf, 0);
      let clusterId = ClusterId.decode(buf, balance.byteLength);
      return {
        balance: balance.value,
        clusterId: clusterId.value
      };
    }
  };

  valueencoding = {
    encode: (key: { }): Buffer => {
      return Buffer.alloc(0);
    },
    decode: (buf: Buffer): {  } => {
      return {};
    }
  };

}  