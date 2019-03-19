import { Router } from 'express';
import { StatusController } from '../controllers/status-controller';
import { expressWrapAsync } from '../misc/utils';

export default function(statusController: StatusController): Router {
  const router = Router();
  router.get('', expressWrapAsync(statusController.getStatus));
  return router;
}