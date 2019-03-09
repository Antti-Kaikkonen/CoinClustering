import { injectable } from "inversify";
import { AddressBalanceTable } from "../tables/address-balance-table";
import { AddressClusterTable } from "../tables/address-cluster-table";

@injectable()
export class AddressService {

  constructor(private addressClusterTable: AddressClusterTable,
    private addressBalanceTable: AddressBalanceTable) {
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