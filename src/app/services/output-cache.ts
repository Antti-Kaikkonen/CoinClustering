import * as lexi from 'lexint';
import { AddressEncodingService } from './address-encoding-service';

//const ADDRESS_BYTE_LENGTH = 21;

//a compact FIFO cache for output values and addresses
export class OutputCache {

  constructor(private maxSize: number, private addressEncodingService: AddressEncodingService) {
  }

  private cache: Map<string, Buffer> = new Map();
  
  public size: number = 0;

  clear() {
    this.cache.clear();
  }

  set(key: {txid: string, n: number}, value: {valueSat: number, addresses: string[]}) {
    let compactKey: string = Buffer.concat([lexi.encode(key.n), Buffer.from(key.txid, 'hex')]).toString('ucs2');
    let addresses = value.addresses;
    if (!addresses) addresses = [];
    let addressesBuffer = Buffer.concat(addresses.map(address => {
      try {
        return this.addressEncodingService.addressToBytes(address);
        //return bs58.decode(address)
      } catch(err) {
        console.log("can't decode ", address, "txid", key.txid, "n", key.n);
        throw err;
      }
    }));
    let compactValue = Buffer.concat([lexi.encode(value.valueSat), addressesBuffer]);
    this.cache.set(compactKey, compactValue);
    if (this.cache.size > this.maxSize) this.cache.delete(this.cache.keys().next().value);//remove oldest
    this.size = this.cache.size;
  }

  del(key: {txid: string, n: number}) {
    let compactKey: string = Buffer.concat([lexi.encode(key.n), Buffer.from(key.txid, 'hex')]).toString('ucs2');
    this.cache.delete(compactKey);
    this.size = this.cache.size;
  }

  get(key: {txid: string, n: number}): {valueSat: number, addresses: string[]} {
    let compactKey: string = Buffer.concat([lexi.encode(key.n), Buffer.from(key.txid, 'hex')]).toString('ucs2');
    
    let compactValue = this.cache.get(compactKey);
    if (compactValue === undefined) return undefined;
    let valueSat = lexi.decode(compactValue);
    let from = valueSat.byteLength;
    let addresses = [];
    while (from < compactValue.length) {
      try {
        let address = this.addressEncodingService.bytesToAddress(compactValue, from);
        let bytes_read = this.addressEncodingService.addressToBytes(address).length;
        addresses.push(address);
        from += bytes_read;
      } catch(err) {
        console.log("outputcache: error decoding "+key.txid+" "+key.n);
        throw err;
      }
      //let address = bs58.encode(compactValue.slice(from, from+ADDRESS_BYTE_LENGTH));
      
    }
    return {
      valueSat: valueSat.value,
      addresses: addresses
    };
  }

}