import { AbstractBatch } from "abstract-leveldown";
import { Writable } from "stream";
import { WriteBatchState, WriteBatchStateTable } from "../tables/write-batch-state-table";
import { WriteBatchTable, WriteType } from "../tables/write-batch-table";
import { BinaryDB } from "./binary-db";

const BATCH_SIZE: number = 1000000;

/**
 * LevelDB and RocksDB process WriteBatch in memory. 
 * The purpose of this class is to allow large WriteBatch operations to spill to disk in order to limit memory usage. 
 * TODO: lock the db before splilling to disk and unlock when fully processed.
 */

export class WriteBatchService {

  ops: AbstractBatch<Buffer, Buffer>[];
  writeBatchTable: WriteBatchTable;
  writeBatchStateTable: WriteBatchStateTable;
  writeBatchState: WriteBatchState;
  state: WriteBatchState;

  constructor(private db: BinaryDB) {
    this.writeBatchStateTable = new WriteBatchStateTable(db);
    this.writeBatchTable = new WriteBatchTable(db);
    this.ops = [];
  }

  private async getWriteBatchState(): Promise<WriteBatchState> {
    if (this.state === undefined) {
      try {
        let res = await this.writeBatchStateTable.get(undefined);
        this.state = res.status;
      } catch(err) {
        if (err.notFound) {
          this.state = "empty";
        } else {
          throw err;
        }  
      }
    }
    return this.state;
  }

  private flushOpsToDbOps() {
    console.log("FlushOpsToDb");
    let ops: AbstractBatch<Buffer, Buffer>[] = this.ops.map(op => {
      if (op.type === "del") {
        return this.writeBatchTable.putOperation({key: op.key}, {type: "del"});
      } else if (op.type === "put") {
        return this.writeBatchTable.putOperation({key: op.key}, {type: "put", value: op.value});
      }
    });
    return ops;
  }

 async push(op: AbstractBatch): Promise<void> {
    this.ops.push(op);
    if (this.ops.length >= BATCH_SIZE) {
      let state = await this.getWriteBatchState();
      let ops = this.flushOpsToDbOps();
      this.ops = [];
      if (state !== "filling") {
        ops.push(this.writeBatchStateTable.putOperation(undefined, { status: 'filling' }));
      }
      await this.db.batchBinary(ops);
      this.state = "filling";
    }
  }

  private async saveWriteBatchTable(): Promise<void> {
    console.log("saveWriteBatchTable");
    let ops: AbstractBatch<Buffer, Buffer>[] = [];
    let writer = new Writable({
      objectMode: true,
      write: async (row: 
      { 
        key: {key: Buffer}, 
        value: {type: WriteType, value?: Buffer}
      }, encoding, callback) => {
        if (ops.length >= BATCH_SIZE) {
          await this.db.batchBinary(ops);
          ops = [];
        }
        let key: Buffer = row.key.key;
        let type: WriteType = row.value.type;
        if (type === "put") {
          ops.push({key: key, type: 'put', value: row.value.value});
        } else if (type === "del") {
          ops.push({key: key, type: 'del'});
        }
        ops.push(
          this.writeBatchTable.delOperation({key: key})
        );
        callback(null);
      }
      }
    );

    return new Promise<void>((resolve, reject) => {
      this.writeBatchTable.createReadStream().pipe(writer).on('finish', async () => {
        ops.push(
          this.writeBatchStateTable.putOperation(undefined, { status: 'empty' })
        );
        await this.db.batchBinary(ops);
        this.state = "empty";
        resolve();
      });
    });
  }

  private async clearWriteBatchTable() {
    let ops: AbstractBatch<Buffer, Buffer>[] = [];

    let writer = new Writable({
      objectMode: true,
      //highWaterMark: 256,
      write: async (row: 
      { 
        key: {key: Buffer}, 
        value: {type: WriteType, value?: Buffer}
      }, encoding, callback) => {
        if (ops.length >= BATCH_SIZE) {
          await this.db.batchBinary(ops);
          ops = [];
        }
        let key: Buffer = row.key.key;
        ops.push(
          this.writeBatchTable.delOperation({key: key})
        );
        callback(null);
      }
    });

    return new Promise<void>((resolve, reject) => {
      this.writeBatchTable.createReadStream().pipe(writer).on('finish', async () => {
        ops.push(
          this.writeBatchStateTable.putOperation(undefined, { status: 'empty' })
        );
        await this.db.batchBinary(ops);
        this.state = 'empty';
        resolve();
      });
    });
  }

  async process() {
    let state = await this.getWriteBatchState();
    if (state === "emptying") {
      console.log("process.. emptying");
      await this.saveWriteBatchTable();
    } else if (state === "filling") {
      console.log("process.. filling");
      await this.clearWriteBatchTable();
    } else if (state === "empty") {
      console.log("process.. empty");
      //do nothing
    }
  }

  async commit() {
    if (await this.getWriteBatchState() === "empty") {
      if (this.ops.length === 0) return;
      await this.db.batchBinary(this.ops);
      this.ops = [];
      return;
    }
    let startOps: AbstractBatch<Buffer, Buffer>[] = this.ops;
    this.ops = [];
    startOps.push(this.writeBatchStateTable.putOperation(undefined, {status: 'emptying'}))
    await this.db.batchBinary(startOps);
    this.state = 'emptying';
    await this.saveWriteBatchTable();
  }

}  