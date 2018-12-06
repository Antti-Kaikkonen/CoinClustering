export const db_cluster_address_prefix = "cluster_address/";//prefix/clusterId/# => address (no particular order)
export const db_cluster_address_count_prefix = "cluster_address_count/";//prefix/clusterId => count
export const db_address_cluster_prefix = "address_cluster/";//prefix/address => clusterId

export const db_cluster_balance_prefix = "cluster_balance_event/";// prefix/clusterid/# => txid;balanceAfter (order # by 1:height, 2:index in block)
export const db_cluster_tx_balance_prefix = "cluster_tx_balance/";//prefix/clusterid/txid => #;balanceAfter;height;n
export const db_cluster_balance_count_prefix = "cluster_balance_count/";


export const db_address_balance_prefix = "address_balance/";// prefix/address/# => txid;balanceAfter;height;n (order # by 1:height, 2:index in block)
export const db_address_tx_balance_prefix = "address_tx_balance/";// prefix/address/txid => #;balanceAfter;height;n
export const db_address_balance_count_prefix = "address_balance_count/";

export const db_next_cluster_id = "next_cluster_id/";
export const db_value_separator = ";";

export const db_block_hash = "block_hash/";//prefix/height => hash