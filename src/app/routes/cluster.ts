import { Router } from 'express';
import { ClusterController } from '../controllers/cluster-controller';

export default function(clusterController: ClusterController): Router {
  const router = Router();
  router.get("/:id/transactions", clusterController.clusterTransactions);
  router.get('/:id/addresses', clusterController.clusterAddresses);
  router.get('/top-:n', clusterController.largestClusters);
  router.get('/tx/:txid', clusterController.txClusterBalnaceChanges);
  return router;
}