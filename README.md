# Storm-HBase
Storm-HBase is a combination of [Storm](https://github.com/nathanmarz/storm) and [HBase](https://github.com/apache/hbase). This library will let you use HBase as a spout within Storm.

## Notes
`HBaseSpout` is used to continuously read stream data from HBase cluster according to the range of [`start_timestamp`, `stop_timestamp`]:
* If `start_timestamp` is set to 0, `HBaseSpout` will read data from 3 minutes ago by default; otherwise it will read data from the specified `start_timestamp`.
* If `stop_timestamp` is set to 0, `HBaseSpout` will read data unit now and keep on reading as time goes on by default; otherwise it will read data unit the specified `stop_timestamp`.

All the configuration options can be found in `src/main/resources/storm.properties` and `src/main/resources/hbase.properties` files. You can also custom them or some of them if necessary.

`HBaseSpout` is based on the following assumptions:
* the rowkey of HBase table consists of [`shardingkey`, `timestamp`, ...].
* the `shardingkey` takes up the 1st byte of rowkey, which means the data partitions number of HBase table and usually is a short type number.
* the `timestamp` takes up the 2nd to 5th bytes of rowkey, which is an UNIX timestamp in second.

## Contributors
* ypf412 (ypf412@163.com)
* jiuling.ypf (jiuling.ypf@taobao.com)
