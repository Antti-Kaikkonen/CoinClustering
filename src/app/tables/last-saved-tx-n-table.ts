import * as lexi from 'lexint';
import { db_last_saved_tx_n } from "../services/db-constants";
import { PrefixTable } from './prefix-table';

export class LastSavedTxNTable extends PrefixTable<undefined, { n: number }> {

  prefix = db_last_saved_tx_n;

  keyencoding = {
    encode: (a): Buffer => {
        return Buffer.from([]);
    },
    decode: (buf: Buffer) => {
      return undefined
    }
  };

  valueencoding = {
    encode: (key: { n: number }): Buffer => {
      return Buffer.from(lexi.encode(key.n));
    },
    decode: (buf: Buffer): { n: number } => {
      return {
        n: lexi.decode(buf).value
      }
    }
  };
}