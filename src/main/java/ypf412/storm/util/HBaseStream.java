package ypf412.storm.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;


/**
 * This is the HBase data generator class.
 * An HBaseStream object will continuously scan a specified partition by a sharding key.
 * 
 * @author jiuling.ypf
 * 
 */
public class HBaseStream {

	private static final Log LOG = LogFactory.getLog(HBaseStream.class);
	
	private static final Configuration conf = HBaseConfiguration.create();

	private transient HTable hTable;

	private short shardingNum;
	
	private String tableName;

	private byte[] columnFamily;
	
	private int cachingRecord;

	private int startTimestamp;
	
	private int stopTimestamp;
	
	/**
	 * Constructor Function
	 * 
	 */
	public HBaseStream() {
		this(0, 0);
	}
	
	/**
	 * Constructor Function
	 * 
	 * @param startTimestamp
	 * @param stopTimestamp
	 */
	public HBaseStream(int startTimestamp, int stopTimestamp) {
		this.shardingNum = Constants.HBASE_DEFAULT_SHARDING_NUM;
		this.tableName = Constants.HBASE_DEFAULT_TABLE_NAME;
		this.columnFamily = Bytes.toBytes(Constants.HBASE_DEFAULT_COLUMN_FAMILY);
		this.cachingRecord = Constants.HBASE_DEFAULT_CACHING_RECORD;
		this.startTimestamp = startTimestamp;
		this.stopTimestamp = stopTimestamp;
		initHTable();
	}
	
	/**
	 * Constructor Function
	 * 
	 * @param hbasePropConfig
	 * @param startTimestamp
	 * @param stopTimestamp
	 */
	public HBaseStream(PropConfig hbasePropConfig, int startTimestamp, int stopTimestamp) {
		this.shardingNum = Constants.HBASE_DEFAULT_SHARDING_NUM;
		String shardingStr = hbasePropConfig.getProperty("hbase.table.sharding");
		if (shardingStr != null)
			shardingNum = Short.valueOf(shardingStr);
		this.tableName = Constants.HBASE_DEFAULT_TABLE_NAME;
		String tableNameStr = hbasePropConfig.getProperty("hbase.table.name");
		if (tableNameStr != null && !tableNameStr.equals(""))
			this.tableName = tableNameStr;
		this.columnFamily = Bytes.toBytes(Constants.HBASE_DEFAULT_COLUMN_FAMILY);
		String columnFamilyStr = hbasePropConfig.getProperty("hbase.table.column_family");
		if (columnFamilyStr != null && !columnFamilyStr.equals(""))
			this.columnFamily = Bytes.toBytes(columnFamilyStr);
		this.cachingRecord = Constants.HBASE_DEFAULT_CACHING_RECORD;
		String cachingRecordStr = hbasePropConfig.getProperty("hbase.table.scanner_cache");
		if (cachingRecordStr != null && !cachingRecordStr.equals(""))
			this.cachingRecord = Integer.valueOf(cachingRecordStr);
		this.startTimestamp = startTimestamp;
		this.stopTimestamp = stopTimestamp;
		initHTable();
	}

	/**
	 * Initialize HTable object for reading data from HBase cluster
	 * 
	 * @return
	 */
	private boolean initHTable() {
		boolean b = true;
		try {
			hTable = new HTable(conf, tableName);
			hTable.setScannerCaching(cachingRecord);
		} catch (IOException e) {
			LOG.error("Catch exception: ", e);
			b = false;
		}
		return b;
	}
	
	/**
	 * According current timestamp, compute the timestamp secInterval seconds ago.
	 * It will return the UNIX timestamp in second.
	 * @param secInterval
	 * @return
	 */
	private int makeTimestamp(int secInterval) {
		long now = new DateTime().getMillis();
		int nowTs = (int)(now /1000);
		return (nowTs - secInterval);
	}
	
	/**
	 * scan HBase table by a specified shardingKey and startKey.
	 * @param sharding
	 * @param startKey
	 * @return
	 */
	public ResultScanner scanTable(short sharding, byte[] startKey) {
		ResultScanner rs = null;
		Scan scan = new Scan();
		scan.addFamily(columnFamily);
		if (sharding >= 0 && sharding < shardingNum) {
			byte bSharding = (byte)sharding;
			
			// Initialize startKey when the first scanning table request
			if (startKey == null) {
				startKey = new byte[5];
				int startTs = 0;
				if (startTimestamp == 0) {
					startTs = makeTimestamp(Constants.HBASE_STREAM_DATA_START_SEC);
				} else {
					startTs = startTimestamp;
				}
				Bytes.putByte(startKey, 0, bSharding);
				Bytes.putInt(startKey, 1, startTs);
			}
			
			// Initialize stopKey
			byte[] stopKey = new byte[5];
			int stopTs = 0;
			if (stopTimestamp == 0) {
				stopTs = makeTimestamp(Constants.HBASE_STREAM_DATA_STOP_SEC);
			} else {
				stopTs = stopTimestamp;
			}
			Bytes.putByte(stopKey, 0, bSharding);
			Bytes.putInt(stopKey, 1, stopTs);
			
			// Set the range of scanning table
			scan.setStartRow(startKey);
			scan.setStopRow(stopKey);
		}
		
		// get result of this scanning process
		try {
			rs = hTable.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Catch exception: ", e);
			rs = null;
		}
		return rs;
	}
	
}
