import { Router } from 'express';
import { TransactionController } from '../controllers/transaction-controller';

export default function(transactionController: TransactionController): Router {
  const router = Router();
  router.get('/:txid/cluster-balance-changes', transactionController.txClusterBalnaceChanges);
  //router.get('/:txid', );TODO
  return router;
}