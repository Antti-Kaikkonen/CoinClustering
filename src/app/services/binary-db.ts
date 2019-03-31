import { AbstractBatch, AbstractLevelDOWN } from "abstract-leveldown";
import EncodingDown from "encoding-down";
import { injectable } from "inversify";
import LevelUp from "levelup";
import { WriteBatchService } from "./write-batch-service";

@injectable()
export class BinaryDB extends LevelUp<AbstractLevelDOWN<Buffer, Buffer>> {

  writeBatchService: WriteBatchService;
  batch: undefined;

  constructor(db: EncodingDown<Buffer, Buffer>, options: any) {
    super(db, options);
    this.writeBatchService = new WriteBatchService(this);
  }

  public batchBinary(array: AbstractBatch<Buffer, Buffer>[], options?: any): Promise<void> {
    return super.batch(array, options);
  }

}