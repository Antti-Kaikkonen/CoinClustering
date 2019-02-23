import * as lexi from 'lexint';
import { db_address_balance_prefix } from "../misc/db-constants";
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

export class AddressBalanceTable extends PrefixTable< { address: string }, 
{ balance: number }> {

  constructor(db: BinaryDB, private addressEncodingService: AddressEncodingService) {
    super(db);
  }

  prefix = db_address_balance_prefix;

  keyencoding = {
    encode: (key: { address: string}): Buffer => {
      return this.addressEncodingService.addressToBytes(key.address);
    },
    decode: (buf: Buffer): { address: string } => {
      return {
        address: this.addressEncodingService.bytesToAddress(buf)
      }
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