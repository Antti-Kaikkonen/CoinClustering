
export const db_cluster_address_prefix = Buffer.alloc(1, 0);//prefix/varint(clusterId)/lexint(#) => address (no particular order)
export const db_cluster_address_count_prefix = Buffer.alloc(1, 1);//prefix/varint(clusterId) => varint(count)
export const db_address_cluster_prefix = Buffer.alloc(1, 2);//prefix/address => varint(clusterId)
export const db_cluster_transaction_prefix = Buffer.alloc(1, 3);// prefix/varint(clusterid)/lexint(#) => txid/varint(balanceAfter) (order # by 1:height, 2:index in block)
export const db_cluster_transaction_count_prefix = Buffer.alloc(1, 4);//prefix/varint(clusterid) => varint(count);
export const db_address_balance_prefix = Buffer.alloc(1, 5);// prefix/address/lexint(#) => txid/varint(balanceAfter)/varint(height)/varint(n) (order # by 1:height, 2:index in block)
export const db_address_transaction_prefix = Buffer.alloc(1, 6);// prefix/address/txid => varint(#)/varint(balanceAfter)/varint(height)/varint(n)
export const db_address_balance_count_prefix = Buffer.alloc(1, 7); //predfix/address => varint(count)
export const db_next_cluster_id = Buffer.alloc(1, 8);// prefix/varint(id)
export const db_block_hash = Buffer.alloc(1, 9);//prefix/height => hash
export const db_last_merged_block_height = Buffer.alloc(1, 10);//prefix => height
export const db_last_saved_tx_height = Buffer.alloc(1, 11);//prefix => height
//export const db_last_saved_tx_n = Buffer.alloc(1, 12);//prefix => n
export const db_cluster_merged_to = Buffer.alloc(1, 13);//prefix/clusterId => clusterId (follow these links to discover current cluster from an old clusterId)
export const db_balace_to_cluster_prefix = Buffer.alloc(1, 14);//
export const db_output_prefix = Buffer.alloc(1, 15);//
export const db_cluster_balance_prefix = Buffer.alloc(1, 16);
export const db_write_batch_state_prefix = Buffer.alloc(1, 17);
export const db_Write_batch_prefix = Buffer.alloc(1, 18);