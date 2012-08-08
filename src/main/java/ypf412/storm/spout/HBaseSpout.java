package ypf412.storm.spout;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import ypf412.storm.util.Constants;
import ypf412.storm.util.HBaseStream;
import ypf412.storm.util.PropConfig;
import ypf412.storm.util.StreamData;

/**
 * This spout is used to continuously read data from a HBase cluster according to <start_timestamp, stop_timestamp>.
 * 		1. If start_timestamp is set to 0, spout will read data from 3 minutes ago by default; otherwise it will read
 * data from the specified start_timestamp.
 * 		2. If stop_timestamp is set to 0, spout will read data unit now by default; otherwise it will read data unit
 * the specified start_timestamp.
 * 
 * The spout is based on the following assumptions:
 * 		1. the rowkey of HBase table consists of <shardingKey, timestamp, ...>.
 * 		2. the shardingKey takes up the 1st byte of rowkey.
 * 		3. the timestamp takes up the 2nd to 5th bytes of rowkey, which is UNIX timestamp in second.
 * 
 * @author jiuling.ypf
 * 
 */
public class HBaseSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 442722253074245161L;
	
	private static final Log LOG = LogFactory.getLog(HBaseSpout.class);
	
	/**
	 * queue for tuples
	 */
	private LinkedBlockingQueue<Values> queue;
	
	/**
	 * colloctor for spout
	 */
	private SpoutOutputCollector collector;

	/**
	 * The start UNIX timestamp in second, and it is 0 by default.
	 */
	private int startTimestamp;
	
	/**
	 * The stop UNIX timestamp in second, and it is 0 by default.
	 */
	private int stopTimestamp;
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {

		this.collector = collector;
		
		String stormPropPath = (String) conf.get(Constants.STORM_PROP_CONF_FILE);
		PropConfig stormPropConfig;
		try {
			stormPropConfig = new PropConfig(stormPropPath);
		} catch (IOException e1) {
			LOG.error("Failed to load properties", e1);
			throw new RuntimeException(e1);
		}
		
		int queueSize = Constants.STORM_SPOUT_DEFAULT_QUEUE_SIZE;
		String queueSizeStr = stormPropConfig.getProperty("storm.spout.queue_size");
		if (queueSizeStr != null)
			queueSize = Integer.valueOf(queueSizeStr);
		this.queue = new LinkedBlockingQueue<Values>(queueSize);

		String startTs = stormPropConfig.getProperty("spout.start_timestamp");
		if (startTs != null) { // scan HBase table from the specified timestamp
			this.startTimestamp = Integer.valueOf(startTs);
		} else { // otherwise scan HBase table from 3 minutes ago
			this.startTimestamp = 0;
		}
		
		String stopTs = stormPropConfig.getProperty("spout.stop_timestamp");
		if (stopTs != null) { // scan HBase table until the specified timestamp
			this.stopTimestamp = Integer.valueOf(stopTs);
		} else { // otherwise scan HBase table unit now
			this.stopTimestamp = 0;
		}
		
		String hbasePropPath = (String) conf
				.get(Constants.HBASE_PROP_CONF_FILE);
		PropConfig hbasePropConfig;
		try {
			hbasePropConfig = new PropConfig(hbasePropPath);
		} catch (IOException e1) {
			LOG.error("Failed to load properties", e1);
			throw new RuntimeException(e1);
		}
		
		short shardingNum = Constants.HBASE_DEFAULT_SHARDING_NUM;
		String shardingStr = hbasePropConfig.getProperty("hbase.table.sharding");
		if (shardingStr != null)
			shardingNum = Short.valueOf(shardingStr);
		
		// According to the task id, decide which sharding partitions this task will deal with.
		// And create a thread for every sharding partition.
		int taskId = context.getThisTaskId();
		String componentId = context.getComponentId(taskId);
		int taskNum = context.getComponentTasks(componentId).size();
		int rem = taskId % taskNum;
		try {
			for (short shardingKey = 0; shardingKey < shardingNum; shardingKey++) {
				if (shardingKey % taskNum == rem) {
					Thread scanThread = new Thread(new ShardScanner(shardingKey, hbasePropConfig));
					scanThread.setDaemon(true);
					scanThread.start();
				}
			}
		} catch (Exception e) {
			LOG.error("failed to create scan thread", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void nextTuple() {
		Values tuple;
		try {
			while ((tuple = queue.take()) != null) {
				collector.emit(tuple);
			}
		} catch (Exception e) {
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sharding", "key", "value"));
	}

	/**
	 * Put a tuple to queue 
	 * 
	 * @param tuple
	 * @return
	 */
	protected void putTuple(Values tuple) {
		try {
			queue.put(tuple);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Scan a single sharding partition and send table data
	 * 
	 */
	private class ShardScanner implements Runnable {

		// HBase stream data generator
		private HBaseStream hbaseStream;
		// the sharding key of HBase table
		private short shardingKey;
		// the rowkey of scanning table last time
		private byte[] lastRowKey;
		
		// column family of HBase table
		private byte[] columnFamily;
		
		// column of HBase table
		private byte[] column;

		// the padding character that will appending to lastRowKey every time when scanning table
		private final byte[] pad = new byte[] { 0 };

		public ShardScanner(short shardingKey, PropConfig hbasePropConfig) throws Exception {
			this.shardingKey = shardingKey;
			this.lastRowKey = null;
			this.columnFamily = Bytes.toBytes(Constants.HBASE_DEFAULT_COLUMN_FAMILY);
			String columnFamilyStr = hbasePropConfig.getProperty("hbase.table.column_family");
			if (columnFamilyStr != null && !columnFamilyStr.equals(""))
				this.columnFamily = Bytes.toBytes(columnFamilyStr);
			this.column = Bytes.toBytes(Constants.HBASE_DEFAULT_COLUMN);
			String columnStr = hbasePropConfig.getProperty("hbase.table.column");
			if (columnStr != null && !columnStr.equals(""))
				this.column = Bytes.toBytes(columnStr);
			this.hbaseStream = new HBaseStream(hbasePropConfig, startTimestamp, stopTimestamp);
		}

		@Override
		public void run() {
			long num = 0L;
			int mode = 1000;
			while (!Thread.interrupted()) {
				long startTime = System.currentTimeMillis();
				ResultScanner rs = hbaseStream.scanTable(
						shardingKey, lastRowKey);
				Result rr = new Result();
				if (rs != null) {
					byte[] rowkey = null;
					byte[] value = null;
					while (rr != null) {
						try {
							rr = rs.next();
						} catch (IOException e) {
							LOG.error("Catch exception: ", e);
						}
						if (rr != null && !rr.isEmpty()) {
							num++;
							rowkey = rr.getRow();
							value = rr.getValue(columnFamily, column);
							short sharding = (short)rowkey[0];
							putTuple(new Values(new Short(sharding), new StreamData(rowkey), new StreamData(value)));
						}
						if (num % mode == 0)
							LOG.info("total records: " + num + ", total scan time: " + (System.currentTimeMillis() - startTime) + " ms");
					}
					rs.close();
					if (rowkey != null)
						lastRowKey = Bytes.add(rowkey, pad);
				}
			}
		}
	}

}
