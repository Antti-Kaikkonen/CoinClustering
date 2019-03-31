import { injectable } from 'inversify';
import { db_Write_batch_prefix } from '../misc/db-constants';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from "./prefix-table";

const BYTE_PUT = 0;
const BYTE_DEL = 1;

@injectable()
export class WriteBatchTable extends PrefixTable<{ key:Buffer }, { type: WriteType, value?: Buffer }> {

  constructor(db: BinaryDB) {
    super(db);
  }

  prefix = db_Write_batch_prefix;

  keyencoding = {
    encode: (key: { key:Buffer } ): Buffer => {
        return key.key;
    },
    decode: (buf: Buffer): { key:Buffer } => {
      return {key: buf}
    }
  };

  valueencoding = {
    encode: (key: { type: WriteType, value?: Buffer }): Buffer => {
      if (key.type === "del") {
        return Buffer.alloc(1, BYTE_DEL);
      } else if (key.type === "put") {
        return Buffer.concat([ Buffer.alloc(1, BYTE_PUT), key.value]);
      }
    },
    decode: (buf: Buffer): { type: WriteType, value?: Buffer } => {
      if (buf[0] === BYTE_PUT) {
        return {
          type: 'put', 
          value: buf.slice(1)
        }
      } else if (buf[0] === BYTE_DEL) {
        return {
          type: 'del'
        }
      }
    }
  };
}  

export type WriteType = "put" | "del";