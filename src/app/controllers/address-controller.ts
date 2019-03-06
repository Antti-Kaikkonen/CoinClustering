import { Request, Response } from 'express';
import { AddressEncodingService } from '../services/address-encoding-service';
import { BinaryDB } from '../services/binary-db';
import { ClusterAddressService } from '../services/cluster-address-service';


export class AddressController {

  private clusterAddressService: ClusterAddressService;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService) {
    this.clusterAddressService = new ClusterAddressService(db, addressEncodingService);
  }  

  clusterId = async (req:Request, res:Response) => {
    let address: string = req.params.address;
    try {
      let clusterId = await this.clusterAddressService.getAddressCluster(address);
      res.send(clusterId.toString());
    } catch(err) {
      res.sendStatus(404);
    }
  };

}  