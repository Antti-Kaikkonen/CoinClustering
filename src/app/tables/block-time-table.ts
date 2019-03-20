import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_block_time_prefix } from "../misc/db-constants";
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class BlockTimeTable extends PrefixTable< { height: number}, { time: number }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_block_time_prefix;

  keyencoding = {
    encode: (key: { height: number}): Buffer => {
        return lexi.encode(key.height);
    },
    decode: (buf: Buffer): { height: number} => {
      let height = lexi.decode(buf).value;
      return {
        height: height
      };
    }
  };

  valueencoding = {
    encode: (key: { time: number }): Buffer => {
      return lexi.encode(key.time);
    },
    decode: (buf: Buffer): { time: number } => {
      return {
        time: lexi.decode(buf).value
      };  
    }
  };
}