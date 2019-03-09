import { AddressBalanceTable } from "../tables/address-balance-table";
import { AddressClusterTable } from "../tables/address-cluster-table";
import { AddressEncodingService } from "./address-encoding-service";
import { BinaryDB } from "./binary-db";

export class AddressService {

  private addressClusterTable: AddressClusterTable;
  private addressBalanceTable: AddressBalanceTable;

  constructor(private db: BinaryDB, addressEncodingService: AddressEncodingService) {
    this.addressClusterTable = new AddressClusterTable(db, addressEncodingService);
    this.addressBalanceTable = new AddressBalanceTable(db, addressEncodingService);
  }  

  async getAddressBalanceDefaultUndefined(address: string): Promise<number> {
    try {
      return (await this.addressBalanceTable.get({address: address})).balance;
    } catch(err) {
      if (err.notFound) {
        return undefined;
      }
      throw err;
    }  
  }

  async getAddressCluster(address: string): Promise<number> {
    return (await this.addressClusterTable.get({address: address})).clusterId;
  }

}  