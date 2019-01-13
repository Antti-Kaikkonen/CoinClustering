declare module 'bech32' {

  export function decode(address: string, limit?: number): {prefix: string, words: number[]};
  export function encode(prefix: string, words?: number[]): string;
  export function toWords(bytes: number[] | Buffer): number[];
  export function fromWords(words: number[]): number[]; 

}  
