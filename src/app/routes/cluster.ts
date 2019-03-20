import { Router } from 'express';
import { ClusterController } from '../controllers/cluster-controller';
import { expressWrapAsync } from '../misc/utils';

export default function(clusterController: ClusterController): Router {
  const router = Router();
  router.get("/:id/transactions", expressWrapAsync(clusterController.clusterTransactions));
  router.get('/:id/addresses', expressWrapAsync(clusterController.clusterAddresses));
  router.get('/:id/summary', expressWrapAsync(clusterController.clusterInfo));
  router.get('/:id/balance-candlesticks', expressWrapAsync(clusterController.candleSticks));
  router.get('', expressWrapAsync(clusterController.clusters));
  return router;
}