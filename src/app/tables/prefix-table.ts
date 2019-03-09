import { AbstractBatch, AbstractIteratorOptions } from "abstract-leveldown";
import { EventEmitter } from "events";
import { injectable } from "inversify";
import { Transform } from "stream";
import { BinaryDB } from "../services/binary-db";

@injectable()
export abstract class PrefixTable<K, V> implements Table<K, V> {

  constructor(private db: BinaryDB) {
  }

  abstract prefix: Buffer;

  abstract keyencoding: EncodeDecode<K>;

  abstract  valueencoding: EncodeDecode<V>;

  private prefixAsBuffer(): Buffer {
    return this.prefix;
  }

  putOperation(key: K, value: V): AbstractBatch<Buffer, Buffer> {
    return {
      type:"put",
      key: this.keyAsBuffer(key),
      value: this.valueencoding.encode(value)
    }
  }

  delOperation(key: K): AbstractBatch<Buffer, Buffer> {
    return {
      type:"del",
      key: this.keyAsBuffer(key)
    }
  }

  private nextBuffer(buffer: Buffer) {
    let res: Buffer;
    if (buffer[buffer.length-1] === 255) {//append 0 to end of buffer
      res = Buffer.alloc(buffer.length+1);
      buffer.copy(res);
    } else {//increase last byte
      res = Buffer.from(buffer);
      res[res.length-1]++;
    }
    return res;
  }

  private keyAsBuffer(key: K): Buffer {
    return Buffer.concat( [this.prefixAsBuffer(), this.keyencoding.encode(key)]);
  }

  public bufferAsKey(buffer: Buffer): K {
    return this.keyencoding.decode(buffer.slice(this.prefix.length));
  }

  async get(key: K): Promise<V> {
    return new Promise<V>((resolve, reject) => {
      this.db.get(this.keyAsBuffer(key)).then(valueAsBuffer => {
        resolve(this.valueencoding.decode(valueAsBuffer));
      }, (err) => {
        err.originalKey = key;
        reject(err);
      })
    });
  }

  async put(key: K, value: V): Promise<void> {
    return this.db.put(this.keyAsBuffer(key), this.valueencoding.encode(value)); 
  }

  async del(key: K,): Promise<void> {
    return this.db.del(this.keyAsBuffer(key)); 
  }

  createReadStream(options?: AbstractIteratorOptions<K>): NodeJS.ReadableStream {
    let bufferOptions: AbstractIteratorOptions<Buffer> = {};
    let decoder: Transform;
    if (!options) {
      bufferOptions.gte = this.prefixAsBuffer();
      bufferOptions.lt = this.nextBuffer(this.prefixAsBuffer());
      decoder = new ReadStreamDecoder(this);
    } else {
      if (options.gt && options.gte) throw Error("Don't specify gt and gte");
      if (options.lt && options.lte) throw Error("Don't specify lt and lte");
      bufferOptions.reverse = options.reverse;
      bufferOptions.values = options.values;
      bufferOptions.keys = options.keys;
      if (options.gt) 
        bufferOptions.gt = this.keyAsBuffer(options.gt);
      else if (options.gte) 
        bufferOptions.gte = this.keyAsBuffer(options.gte);
      else 
        bufferOptions.gte = this.prefixAsBuffer();


      if (options.lt) 
        bufferOptions.lt = this.keyAsBuffer(options.lt);
      else if (options.lte) 
        bufferOptions.lte = this.keyAsBuffer(options.lte);
      else bufferOptions.lt = this.nextBuffer(this.prefixAsBuffer())

      if (options.reverse !== undefined) bufferOptions.reverse = options.reverse;
      if (options.limit !== undefined) bufferOptions.limit = options.limit;
      bufferOptions.keyAsBuffer = true;
      bufferOptions.valueAsBuffer = true;
      bufferOptions.keys = options.keys;
      bufferOptions.values = options.values
      if (options.keys !== false && options.values === false) {
        decoder = new KeyStreamDecoder(this);
      } else if (options.keys === false && options.values !== false) {
        decoder = new ValueStreamDecoder(this);
      } else {
        decoder = new ReadStreamDecoder(this);
      }
    }
    return this.db.createReadStream(bufferOptions).pipe(decoder);
  }
}

class ValueStreamDecoder<K, V> extends Transform implements EventEmitter {
  constructor(private table: PrefixTable<K, V>) {
    super({
      objectMode: true,
      transform: (value: Buffer, encoding, callback) => {
        this.push(
          this.table.valueencoding.decode(value)
        );
        callback();
      }
    });
  }
}

class KeyStreamDecoder<K, V> extends Transform implements EventEmitter {
  constructor(private table: PrefixTable<K, V>) {
    super({
      objectMode: true,
      transform: (key: Buffer, encoding, callback) => {
        this.push(
          this.table.bufferAsKey(key)
        );
        callback();
      }
    });
  }
}

class ReadStreamDecoder<K, V> extends Transform implements EventEmitter {
  constructor(private table: PrefixTable<K, V>) {
    super({
      objectMode: true,
      transform: (value: {key: Buffer, value: Buffer}, encoding, callback) => {
        this.push({
          key: table.bufferAsKey(value.key), 
          value: this.table.valueencoding.decode(value.value)
        });
        callback();
      }
    });
  }
}

interface Table<K, V> {
  keyencoding: EncodeDecode<K>,
  valueencoding: EncodeDecode<V>
}

interface EncodeDecode<T> {
  encode(value: T): Buffer;
  decode(buff: Buffer): T
}