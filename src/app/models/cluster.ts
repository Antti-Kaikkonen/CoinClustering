export class Cluster {
  constructor(
    public addresses: Set<string> = new Set(), 
    public clusterIds: Set<number> = new Set()
  ) {}

  mergeFrom(anotherCluster: Cluster) {
    anotherCluster.addresses.forEach(address => this.addresses.add(address));
    anotherCluster.clusterIds.forEach(clusterId => this.clusterIds.add(clusterId));
  }

  intersectsWith(anotherCluster: Cluster): boolean {
    for (const address of this.addresses) {
      if (anotherCluster.addresses.has(address)) return true;
    }
    for (const clusterId of this.clusterIds) {
      if (anotherCluster.clusterIds.has(clusterId)) return true;
    }
    return false;
  }

  clusterIdsSorted(): number[] {
    return Array.from(this.clusterIds).sort((a, b) => a-b);
  }

}