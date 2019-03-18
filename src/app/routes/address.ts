import { Router } from 'express';
import { AddressController } from '../controllers/address-controller';
import { expressWrapAsync } from '../misc/utils';

export default function(addressController: AddressController): Router {
  const router = Router();
  router.get("/:address/cluster_id", expressWrapAsync(addressController.clusterId));
  router.get("/:address/transactions", expressWrapAsync(addressController.addressTransactions));
  return router;
}