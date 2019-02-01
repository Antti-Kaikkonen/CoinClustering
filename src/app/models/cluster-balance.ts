export class ClusterTransaction {


  constructor(public txid?: string,
  public balanceDelta?: number,
  public height?: number,
  public n?: number) {
  }  
}