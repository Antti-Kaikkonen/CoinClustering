import bech32 from 'bech32';
import bs58 from 'bs58';
import crypto from 'crypto';
import { inject, injectable, named } from "inversify";
import "reflect-metadata";

//byte format will be prefixed with one of these
const TYPE_PUBKEYHASH = 0;//20 bytes
const TYPE_SCRIPTHASH = 1;//20 bytes
const TYPE_witness_v0_keyhash = 2;//20 bytes
const TYPE_witness_v0_scripthash = 3;//32 bytes

function calculateDoubleSha256CheckSum(data: Buffer): Buffer {
  let first = crypto.createHash('sha256').update(data).digest();
  let second = crypto.createHash('sha256').update(first).digest();
  return second.slice(0, 4);
}

@injectable()
export class AddressEncodingService {

  constructor(
    @inject("number") @named("pubkeyhash") private pubkeyhash: number, 
    @inject("number") @named("scripthash") private scripthash: number, 
    @inject("string") @named("segwitprefix") private segwitprefix?: string) {
  }

  addressToBytes(address: string): Buffer {
    if (this.segwitprefix && address.startsWith(this.segwitprefix) && address.length > 35) {
      let decoded = bech32.decode(address);
      let version = decoded.words[0];
      if (version !== 0) throw new Error("Only witness version 0 supported");
      let data = decoded.words.slice(1);
      let witness_program_data = bech32.fromWords(data);
      if (witness_program_data.length === 20) {
        return Buffer.from([TYPE_witness_v0_keyhash].concat(witness_program_data));
      } else if (witness_program_data.length === 32) {
        return Buffer.from([TYPE_witness_v0_scripthash].concat(witness_program_data));
      } else {
        throw new Error("Invalid witness program data length ("+witness_program_data.length+") for address "+address);
      }
    } else {
      let decoded = bs58.decode(address);
      let prefix = decoded[0];
      let without_checksum = decoded.slice(0, decoded.length-4);
      if (prefix === this.pubkeyhash) {
        without_checksum[0] = TYPE_PUBKEYHASH;
        return without_checksum;
      } else if (prefix === this.scripthash) {
        without_checksum[0] = TYPE_SCRIPTHASH;
        return without_checksum;
      } else {
        throw new Error("Unrecognized address prefix ( "+prefix+" ) for address " + address);
      }
    }
  }
  
  bytesToAddress(bytes: Buffer, offset: number = 0): string {
    let type = bytes[offset];
    if (type === TYPE_PUBKEYHASH) {
      let without_checksum = bytes.slice(offset, offset+21);
      without_checksum[0] = this.pubkeyhash;
      let checksum = calculateDoubleSha256CheckSum(without_checksum);
      return bs58.encode(Buffer.concat([without_checksum, checksum]));
    } else if (type === TYPE_SCRIPTHASH) {
      let without_checksum = bytes.slice(offset, offset+21);
      without_checksum[0] = this.scripthash;
      let checksum = calculateDoubleSha256CheckSum(without_checksum);
      return bs58.encode(Buffer.concat([without_checksum, checksum]));
    } else if (type === TYPE_witness_v0_keyhash) {
      if (!this.segwitprefix) throw new Error("Can't encode without segwitprefix")
      let witness_program_data = bytes.slice(offset+1, offset+21);
      return bech32.encode(this.segwitprefix, [0].concat(bech32.toWords(witness_program_data)));
    } else if (type === TYPE_witness_v0_scripthash) {
      if (!this.segwitprefix) throw new Error("Can't encode without segwitprefix")
      let witness_program_data = bytes.slice(offset+1, offset+33);
      return bech32.encode(this.segwitprefix, [0].concat(bech32.toWords(witness_program_data)));
    }
  }

}