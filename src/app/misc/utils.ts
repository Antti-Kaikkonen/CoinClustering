import { Transaction } from "../models/transaction";

export function JSONtoAmount(value: number): number {
  return Math.round(1e8 * value);
}

export const expressWrapAsync = fn => (req, res, next) => {
  fn(req,res).catch((error) => next(error));
};

export function txAddressesToCluster(tx: Transaction): Set<string> {
  let result = new Set<string>();
  if (isMixingTx(tx)) return result;
  tx.vin.map(vin => vin.address).filter(address => address !== undefined).forEach(address => result.add(address));
  return result;
}

export function txAddresses(tx: Transaction): Set<string> {
  let result = new Set<string>();
  tx.vin.map(vin => vin.address).filter(address => address !== undefined).forEach(address => result.add(address));
  tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1 && vout.scriptPubKey.addresses[0])
  .map(vout => vout.scriptPubKey.addresses[0]).forEach(address => result.add(address));
  return result;
}

export function txInputAddresses(tx: Transaction): Array<string> {
  let result = [];
  tx.vin.map(vin => vin.address).filter(address => address !== undefined).forEach(address => result.push(address));
  return result;
}

export function txOutputAddresses(tx: Transaction): Array<string> {
  let result = [];
  tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1 && vout.scriptPubKey.addresses[0])
  .map(vout => vout.scriptPubKey.addresses[0]).forEach(address => result.push(address));
  return result;
}

export function isMixingTx(tx: Transaction) {
  if (tx.vin.length < 2) return false;
  if (tx.vout.length !== tx.vin.length) return false;
  let firstInput = tx.vin[0];
  if (typeof firstInput.value !== 'number') return false;
  if (!tx.vin.every(vin => vin.value === firstInput.value)) return false;
  if (!tx.vout.every(vout => vout.value === firstInput.value)) return false;
  return true;
}

export function txAddressBalanceChanges(tx: Transaction): Map<string, number> {
  let addressToDelta = new Map<string, number>();
  tx.vin.filter(vin => vin.address)
  .forEach(vin => {
    let oldBalance = addressToDelta.get(vin.address);
    if (!oldBalance) oldBalance = 0;
    addressToDelta.set(vin.address, oldBalance-JSONtoAmount(vin.value));
  }); 
  tx.vout.filter(vout => vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.length === 1)
  .forEach(vout => {
    let oldBalance = addressToDelta.get(vout.scriptPubKey.addresses[0]);
    if (!oldBalance) oldBalance = 0;
    addressToDelta.set(vout.scriptPubKey.addresses[0], oldBalance+JSONtoAmount(vout.value));
  });
  return addressToDelta;
}