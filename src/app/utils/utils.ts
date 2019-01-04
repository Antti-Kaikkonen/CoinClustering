//import lexi from 'lexicographic-integer';

/*export function integer2LexString(number: number): string {
  return lexi.pack(number, 'hex');
}

export function lexString2Integer(str: string): number {
  return lexi.unpack(str);
}*/

export function uint2Buffer(int: number): Buffer {
  if (!Number.isInteger(int)) Error("Not an integer");
  if (int < 0) throw Error("Integer is not unsigned");
  let buf = Buffer.allocUnsafe(4);
  buf.writeInt32BE(int, 0);
  return buf;
}

export function writeNumber2Buffer(int: number, buf: Buffer, offset: number): number {
  return buf.writeUIntBE(int, offset, bytelenth(int));
};

export function number2Hex(number: number): string {
  let hexString = number.toString(16);
  if (hexString.length % 2) {
    hexString = '0' + hexString;
  }
  return hexString;
}

export function number2Buffer(number: number): Buffer {
  return Buffer.from(number2Hex(number), 'hex');
}


function bytelenth(number): number {
  if (number < Math.pow(2, 8*1)) return 1;
  if (number < Math.pow(2, 8*2)) return 2;
  if (number < Math.pow(2, 8*3)) return 3;
  if (number < Math.pow(2, 8*4)) return 4;
  if (number < Math.pow(2, 8*5)) return 5;
  if (number < Math.pow(2, 8*6)) return 6;
  throw Error("Number is too large");
}

export function buf2Uint(buf: Buffer): number {
  return buf.readUInt32BE(0);
}