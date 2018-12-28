import { Transaction } from "./transaction";

abstract class BlockWithoutTransactions {

  hash: string;
  confirmations: number;
  strippedsize?: number;
  size: number;
  weight?: number;
  height: number;
  version: number;
  versionHex?: string;
  merkleroot: string;
  time: number;
  mediantime?: number;
  nonce: number;
  bits: string;
  difficulty: number;
  chainwork: string;
  nTx?: number;
  previousblockhash?: string;
  nextblockhash?: string;
} 

export class Block extends BlockWithoutTransactions {
  tx: string[];
}  


export class BlockWithTransactions extends BlockWithoutTransactions {
  constructor(block: BlockWithoutTransactions, public tx: Transaction[]) {
    super();
    Object.assign(this, block);
    this.tx = tx;
  }

}  