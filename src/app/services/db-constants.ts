export const db_cluster_address_prefix = "a";//prefix/clusterId/# => address (no particular order)
export const db_cluster_address_count_prefix = "b";//prefix/clusterId => count
export const db_address_cluster_prefix = "c";//prefix/address => clusterId

export const db_cluster_balance_prefix = "d";// prefix/clusterid/# => txid;balanceAfter (order # by 1:height, 2:index in block)
export const db_cluster_tx_balance_prefix = "e";//prefix/clusterid/txid => #;balanceAfter;height;n
export const db_cluster_balance_count_prefix = "f";


export const db_address_balance_prefix = "g";// prefix/address/# => txid;balanceAfter;height;n (order # by 1:height, 2:index in block)
export const db_address_tx_balance_prefix = "h";// prefix/address/txid => #;balanceAfter;height;n
export const db_address_balance_count_prefix = "i";

export const db_next_cluster_id = "j";
export const db_value_separator = ";";

export const db_block_hash = "k";//prefix/height => hash

export const db_last_merged_block_height = "l";//prefix => height

export const db_last_saved_tx_height = "m";//prefix => height

export const db_last_saved_tx_n = "n";//prefix => n

export const db_cluster_merged_to = "o";//prefix/clusterId => clusterId (follow these links to discover current cluster from an old clusterId)