export class Transaction {
  txid: string;
  hash?: string;
  version: number;
  size: number;
  vzize?: number;
  weight?: number;
  locktime: number;
  vin: TransactionInput[];
  vout: TransactionOutput[];
}

export class TransactionInput {
  coinbase?: string;
  txid?: string;
  vout?: number;
  scriptSig: {
    asm: string;
    hex: string;
  }
  sequence: number;
  value?: number;
  valueSat?: number;
  address?: string;
} 

export class TransactionOutput {
  value: number;
  n: number;
  scriptPubKey: {
    asm: string;
    hex: string;
    reqSigs: number;
    type: string;
    addresses: string[];
  }
  valueSat?: number;
}  