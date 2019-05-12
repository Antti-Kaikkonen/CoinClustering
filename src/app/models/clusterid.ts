import * as lexi from 'lexint';

export class ClusterId {

  constructor(
    public height: number,
    public txN: number,
    public outputN: number) {
  }  

  encode(): Buffer {
    return Buffer.concat([lexi.encode(this.height), lexi.encode(this.txN), lexi.encode(this.outputN)]);
  }

  compareTo(b: ClusterId): number {
    if (this.height === b.height) {
      if (this.txN === b.txN) {
        if (this.outputN < b.outputN) return -1; else return 1;
      }
      if (this.txN < b.txN) return -1; else return 1;
    }
    if (this.height < b.height) return -1; else return 1;
  }


  static decode(buf: Buffer, offset: number = 0): {value: ClusterId, byteLength: number} {
    let height = lexi.decode(buf, offset);
    offset += height.byteLength;
    let txN = lexi.decode(buf, offset);
    offset += txN.byteLength;
    let outputN = lexi.decode(buf, offset);
    return {
      value: new ClusterId(height.value, txN.value, outputN.value), 
      byteLength: height.byteLength+txN.byteLength+outputN.byteLength
    };
  }

  toString() {
    return ""+this.height+"-"+this.txN+"-"+this.outputN
  }

  static fromString(str: string) {
    let components = str.split("-");
    let res = new ClusterId(Number.parseInt(components[0]), Number.parseInt(components[1]), Number.parseInt(components[2]));
    return res;
  }
  
}