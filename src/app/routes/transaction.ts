import { Router } from 'express';
import { TransactionController } from '../controllers/transaction-controller';
import { expressWrapAsync } from '../misc/utils';

export default function(transactionController: TransactionController): Router {
  const router = Router();
  router.get('/:txid/cluster-balance-changes', expressWrapAsync(transactionController.txClusterBalnaceChanges));
  router.get('/:txid/details', expressWrapAsync(transactionController.txDetailed));
  return router;
}