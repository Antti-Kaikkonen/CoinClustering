declare module 'lexicographic-integer' {
  export function pack(integer: number, encoding: 'hex'): string;
  export function pack(integer: number, encoding?: 'array'): Buffer;
  export function unpack(str: string | Buffer): number;
}