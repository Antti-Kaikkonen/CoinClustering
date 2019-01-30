export class ClusterTransaction {


  constructor(public txid?: string,
  public balanceDeltaSat?: number,
  public height?: number,
  public n?: number) {
  }  
}