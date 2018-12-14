import lexi from 'lexicographic-integer';

export function integer2LexString(number: number): string {
  return lexi.pack(number, 'hex');
  /*let result = ""+number;
  while (result.length < 14) {
    result = "0"+result;
  }
  return result;*/
}

export function lexString2Integer(str: string): number {
  return lexi.unpack(str);
}