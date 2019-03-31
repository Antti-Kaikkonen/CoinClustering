export class Cluster {
  
  constructor(
    public addresses: string[] = [], 
    public clusterIds: number[] = []
  ) {}

  mergeFrom(anotherCluster: Cluster) {
    anotherCluster.addresses.forEach(address => this.addresses.push(address));
    anotherCluster.clusterIds.forEach(clusterId => this.clusterIds.push(clusterId));
  }

  /*intersectsWith(anotherCluster: Cluster): boolean {
    for (const address of this.addresses) {
      if (anotherCluster.addresses.has(address)) return true;
    }
    for (const clusterId of this.clusterIds) {
      if (anotherCluster.clusterIds.has(clusterId)) return true;
    }
    return false;
  }*/

  clusterIdsSorted(): number[] {
    return this.clusterIds.sort((a, b) => a-b);
  }

}