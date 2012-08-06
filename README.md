# Storm-HBase
Storm-HBase is a combination of Storm and HBase. This library will let you use HBase as a spout within Storm.

## Notes
This spout is used to continuously read data from a HBase cluster according to [start_timestamp, stop_timestamp].
* If start_timestamp is set to 0, spout will read data from 3 minutes ago by default; otherwise it will read data from the specified start_timestamp.
* If stop_timestamp is set to 0, spout will read data unit now by default; otherwise it will read data unit the specified start_timestamp.

The spout is based on the following assumptions:
* the rowkey of HBase table consists of [shardingKey, timestamp, ...].
* the shardingKey takes up the 1st byte of rowkey.
* the timestamp takes up the 2nd to 5th bytes of rowkey, which is an UNIX timestamp in second.

## Contributors
* ypf412 (ypf412@163.com)
