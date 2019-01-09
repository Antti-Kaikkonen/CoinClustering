import bs58 from 'bs58';
import * as lexi from 'lexint';

const ADDRESS_BYTE_LENGTH = 25;

//a compact FIFO cache for output values and addresses
export class OutputCache {

  constructor(private maxSize: number) {
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
    let addressesBuffer = Buffer.concat(addresses.map(address => bs58.decode(address)));
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
      let address = bs58.encode(compactValue.slice(from, from+ADDRESS_BYTE_LENGTH));
      addresses.push(address);
      from += ADDRESS_BYTE_LENGTH;
    }
    return {
      valueSat: valueSat.value,
      addresses: addresses
    };
  }

}