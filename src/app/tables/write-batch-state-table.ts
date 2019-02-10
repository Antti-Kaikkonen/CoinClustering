import { db_write_batch_state_prefix } from "../services/db-constants";
import { PrefixTable } from "./prefix-table";

const BYTE_EMPTY = 0;
const BYTE_FILLING = 1;
const BYTE_EMPTYING = 2;

export class WriteBatchStateTable extends PrefixTable<undefined, {status: WriteBatchState}> {

  prefix = db_write_batch_state_prefix;

  keyencoding = {
    encode: (key ): Buffer => {
        return Buffer.alloc(0);
    },
    decode: (buf: Buffer) => undefined
  };

  valueencoding = {
    encode: (key: {status: WriteBatchState}): Buffer => {
      if (key.status === "empty") {
        return Buffer.alloc(1, BYTE_EMPTY);
      } else if (key.status === "filling") {
        return Buffer.alloc(1, BYTE_FILLING);
      } else if (key.status === "emptying") {
        return Buffer.alloc(1, BYTE_EMPTYING);
      }
    },
    decode: (buf: Buffer): {status: WriteBatchState} => {
      if (buf[0] === BYTE_EMPTY) {
        return {
          status: 'empty'
        }
      } else if (buf[0] === BYTE_FILLING) {
        return {
          status: 'filling'
        }
      } else if (buf[0] === BYTE_EMPTYING) {
        return {
          status: 'emptying'
        }
      }
    }
  };

}  

export type WriteBatchState = "filling" | "emptying" | "empty";