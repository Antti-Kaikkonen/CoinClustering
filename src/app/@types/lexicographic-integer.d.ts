declare module 'lexicographic-integer' {
  export function pack(integer: number, encoding?: string): string;
  export function unpack(str: string): number;
}