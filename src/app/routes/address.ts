import { Router } from 'express';
import { AddressController } from '../controllers/address-controller';

export default function(addressController: AddressController): Router {
  const router = Router();
  router.get("/:address/cluster_id", addressController.clusterId);
  return router;
}