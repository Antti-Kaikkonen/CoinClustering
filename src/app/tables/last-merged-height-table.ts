import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_last_merged_block_height } from "../misc/db-constants";
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class LastMergedHeightTable extends PrefixTable<undefined, { height: number }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_last_merged_block_height;

  keyencoding = {
    encode: (a): Buffer => {
        return Buffer.from([]);
    },
    decode: (buf: Buffer) => {
      return undefined
    }
  };

  valueencoding = {
    encode: (key: { height: number }): Buffer => {
      return Buffer.from(lexi.encode(key.height));
    },
    decode: (buf: Buffer): { height: number } => {
      return {
        height: lexi.decode(buf).value
      }
    }
  };
}