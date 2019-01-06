
export const db_cluster_address_prefix = "a";//prefix/varint(clusterId)/lexint(#) => address (no particular order)
//keyencoding => decode(buf:Buffer): {...}, encode({...}): Buffer
//valueencoding => decode(buf:Buffer): {...}, encode({...}): Buffer


export const db_cluster_address_count_prefix = "b";//prefix/varint(clusterId) => varint(count)
export const db_address_cluster_prefix = "c";//prefix/address => varint(clusterId)

export const db_cluster_balance_prefix = "d";// prefix/varint(clusterid)/lexint(#) => txid/varint(balanceAfter) (order # by 1:height, 2:index in block)
export const db_cluster_tx_balance_prefix = "e";//prefix/varint(clusterid)/txid => varint(#)/varint(balanceAfter)/varint(height)/varint(n)
export const db_cluster_balance_count_prefix = "f"//prefix/varint(clusterid) => varint(count);


export const db_address_balance_prefix = "g";// prefix/address/lexint(#) => txid/varint(balanceAfter)/varint(height)/varint(n) (order # by 1:height, 2:index in block)
export const db_address_tx_balance_prefix = "h";// prefix/address/txid => varint(#)/varint(balanceAfter)/varint(height)/varint(n)
export const db_address_balance_count_prefix = "i"; //predfix/address => varint(count)

export const db_next_cluster_id = "j";// prefix/varint(id)
export const db_value_separator = ";";

export const db_block_hash = "k";//prefix/height => hash

export const db_last_merged_block_height = "l";//prefix => height

export const db_last_saved_tx_height = "m";//prefix => height

export const db_last_saved_tx_n = "n";//prefix => n

export const db_cluster_merged_to = "o";//prefix/clusterId => clusterId (follow these links to discover current cluster from an old clusterId)

export const db_balace_to_cluster_prefix = "p";//