import * as lexi from 'lexint';
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { db_output_prefix } from '../services/db-constants';
import { PrefixTable } from './prefix-table';

export class OutputCacheTable extends PrefixTable<{txid: string, n: number}, { addresses: string[], valueSat: number }> {

  constructor(db: BinaryDB, private addressEncodingService: AddressEncodingService) {
    super(db);
  }

  prefix = db_output_prefix;

  keyencoding = {
    encode: (key: {txid: string, n: number}): Buffer => {
      return Buffer.concat([lexi.encode(key.n), Buffer.from(key.txid, 'hex')]);
    },
    decode: (buf: Buffer): {txid: string, n: number} => {
      let n = lexi.decode(buf);
      let txid = buf.toString('hex', n.byteLength);
      return {
        n: n.value, 
        txid: txid
      }
    }
  };

  valueencoding = {
    encode: (value: { addresses: string[], valueSat: number }): Buffer => {
      let addresses = value.addresses;
      if (!addresses) addresses = [];
      let addressesBuffer = Buffer.concat(addresses.map(address => this.addressEncodingService.addressToBytes(address)));
      let compactValue = Buffer.concat([lexi.encode(value.valueSat), addressesBuffer]);
      return compactValue;
    },
    decode: (buf: Buffer): { addresses: string[], valueSat: number } => {

      let valueSat = lexi.decode(buf);
      let from = valueSat.byteLength;
      let addresses = [];
      while (from < buf.length) {
        let address = this.addressEncodingService.bytesToAddress(buf, from);
        let bytes_read = this.addressEncodingService.addressToBytes(address).length;
        addresses.push(address);
        from += bytes_read;
      }

      return {
        addresses: addresses,
        valueSat: valueSat.value
      }
    }
  };
}