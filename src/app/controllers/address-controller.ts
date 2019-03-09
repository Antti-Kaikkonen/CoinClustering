import { Request, Response } from 'express';
import { AddressEncodingService } from '../services/address-encoding-service';
import { AddressService } from '../services/address-service';
import { BinaryDB } from '../services/binary-db';


export class AddressController {

  private addressService: AddressService;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService) {
    this.addressService = new AddressService(this.db, addressEncodingService);
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