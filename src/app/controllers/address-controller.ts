import { Request, Response } from 'express';
import { injectable } from 'inversify';
import { AddressService } from '../services/address-service';

@injectable()
export class AddressController {

  constructor(private addressService: AddressService) {
  }  

  clusterId = async (req:Request, res:Response) => {
    let address: string = req.params.address;
    try {
      let clusterId = await this.addressService.getAddressCluster(address);
      res.send(clusterId.toString());
    } catch(err) {
      res.sendStatus(404);
    }
  };

}  