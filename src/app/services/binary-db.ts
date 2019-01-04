import { AbstractBatch, AbstractLevelDOWN } from "abstract-leveldown";
import LevelUp from "levelup";

export class BinaryDB extends LevelUp<AbstractLevelDOWN<Buffer, Buffer>> {

  batch: undefined;

  public batchBinary(array: AbstractBatch<Buffer, Buffer>[], options?: any): Promise<void> {
    return super.batch(array, options);
  }

}