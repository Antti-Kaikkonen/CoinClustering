import lexi from 'lexicographic-integer';

export function integer2LexString(number: number): string {
  return lexi.pack(number, 'hex');
}

export function lexString2Integer(str: string): number {
  return lexi.unpack(str);
}