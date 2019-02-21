import * as lexi from 'lexint';
import { db_last_saved_tx_height } from "../misc/db-constants";
import { PrefixTable } from './prefix-table';

export class LastSavedTxHeightTable extends PrefixTable<undefined, { height: number }> {

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