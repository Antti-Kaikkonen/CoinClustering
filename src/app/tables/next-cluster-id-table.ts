import * as lexi from 'lexint';
import { db_next_cluster_id } from "../misc/db-constants";
import { PrefixTable } from './prefix-table';

export class NextClusterIdTable extends PrefixTable<undefined, { nextClusterId: number }> {

  prefix = db_next_cluster_id;

  keyencoding = {
    encode: (a): Buffer => {
        return Buffer.from([]);
    },
    decode: (buf: Buffer) => {
      return undefined
    }
  };

  valueencoding = {
    encode: (key: { nextClusterId: number }): Buffer => {
      return Buffer.from(lexi.encode(key.nextClusterId));
    },
    decode: (buf: Buffer): { nextClusterId: number } => {
      return {
        nextClusterId: lexi.decode(buf).value
      }
    }
  };
}