import { ClusterId } from "./clusterid";

export class Cluster {

  constructor(
    public addresses: string[] = [], 
    public clusterIds: ClusterId[] = [],
    public lowestClusterId?: ClusterId,
  ) {}

  mergeFrom(anotherCluster: Cluster) {
    anotherCluster.addresses.forEach(address => this.addresses.push(address));
    anotherCluster.clusterIds.forEach(clusterId => this.clusterIds.push(clusterId));
    this.updateLowestClusterId(anotherCluster.lowestClusterId);
  }

  updateLowestClusterId(ci: ClusterId) {
    if (this.lowestClusterId === undefined || (ci !== undefined && ci.compareTo(this.lowestClusterId) < 0)) {
      this.lowestClusterId = ci;
    }
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

  clusterIdsSorted(): ClusterId[] {
    return this.clusterIds.sort((a, b) => a.compareTo(b));
  }

}