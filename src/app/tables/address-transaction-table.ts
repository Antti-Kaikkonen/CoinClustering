import * as lexi from 'lexint';
import { db_address_transaction_prefix } from '../misc/db-constants';
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { PrefixTable } from './prefix-table';

const TXID_BYTE_LENGTH = 32;

export class AddressTransactionTable extends PrefixTable< { address: string, height: number, n: number }, { txid: string, balance: number }> {

  constructor(db: BinaryDB, private addressEncodingService: AddressEncodingService) {
    super(db);
  }

  prefix = db_address_transaction_prefix;

  keyencoding = {
    encode: (key: { address: string, height: number, n: number }): Buffer => {
      let addressBytes = this.addressEncodingService.addressToBytes(key.address);
      let heightBytes = lexi.encode(key.height);
      let nBytes = lexi.encode(key.n);
      return Buffer.concat([addressBytes, heightBytes, nBytes]);
        //return bs58.decode(key.address);
    },
    decode: (buf: Buffer): { address: string, height: number, n: number } => {
      let offset = 0;
      let address = this.addressEncodingService.bytesToAddress(buf, offset);
      offset += this.addressEncodingService.addressToBytes(address).length;
      let height = lexi.decode(buf, offset);
      offset += height.byteLength;
      let n = lexi.decode(buf, offset);
      return {
        address: address,
        height: height.value,
        n: n.value
        //address: bs58.encode(buf)
      }
    }
  };

  valueencoding = {
    encode: (key: { txid: string, balance: number }): Buffer => {
      let txidBytes = Buffer.from(key.txid, 'hex');
      if (txidBytes.length !== TXID_BYTE_LENGTH) throw Error("TXID must be " + TXID_BYTE_LENGTH +" bytes");
      let balanceBytes = lexi.encode(key.balance);
      return Buffer.concat([txidBytes, balanceBytes]);
    },
    decode: (buf: Buffer): { txid: string, balance: number } => {
      let txid = buf.toString('hex', 0, TXID_BYTE_LENGTH);
      let balance = lexi.decode(buf, TXID_BYTE_LENGTH);
      return {
        txid: txid,
        balance: balance.value
      };
    }
  };
}