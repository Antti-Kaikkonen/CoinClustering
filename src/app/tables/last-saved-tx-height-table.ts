import { injectable } from 'inversify';
import * as lexi from 'lexint';
import { db_last_saved_tx_height } from "../misc/db-constants";
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

@injectable()
export class LastSavedTxHeightTable extends PrefixTable<undefined, { height: number }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_last_saved_tx_height;

  keyencoding = {
    encode: (a): Buffer => {
        return Buffer.alloc(0);
    },
    decode: (buf: Buffer) => {
      return undefined
    }
  };

  valueencoding = {
    encode: (key: { height: number }): Buffer => {
      return lexi.encode(key.height);
    },
    decode: (buf: Buffer): { height: number } => {
      return {
        height: lexi.decode(buf).value
      }
    }
  };
}