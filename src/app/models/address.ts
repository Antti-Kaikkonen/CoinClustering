class Address {

  //DASH pubkeyhash: X, scripthash: 7
  //Bitcoin pubkeyhash: 1, scripthash: 3, witness_v0_keyhash: bc1q, multisig: 1 (array)

  type: "nonstandard" |  "pubkey" | "pubkeyhash" | "witness_v0_keyhash" | "witness_v0_scripthash" | "witness_unknown"
  address: string
}
