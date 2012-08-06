package ypf412.storm.util;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * This is the HBase dumper class.
 * An HBaseDumper object will dump data to the specified HBase table, column Family and column.
 * 
 * @author jiuling.ypf
 */
public class HBaseDumper {

	private static final Log LOG = LogFactory.getLog(HBaseDumper.class);

	private static final Configuration conf = HBaseConfiguration.create();
	
	private String tableName;

	private byte[] columnFamily;
	
	private byte[] column;

	private static final Random rand = new Random();

	private static final int tableN = 5;

	private static HTable wTables[];
	
	/**
	 * Constructor Function
	 */
	public HBaseDumper(String tableName, String columnFamily, String column) {
		this.tableName = tableName;
		this.columnFamily = Bytes.toBytes(columnFamily);
		this.column = Bytes.toBytes(column);
		initHTables();
		startDaemon();
	}
	
	/**
	 * Insert one record into HBase table
	 * 
	 * @param rowkey
	 * @param value
	 * @return
	 */
	public boolean writeToTable(byte[] rowkey, byte[] value) {
		Put put = getPutForTable(rowkey, value);
		if (wTables == null) {
			LOG.error("HTable initialized error, terminate program");
			return false;
		}
		int tmp = rand.nextInt(tableN);
		synchronized (wTables[tmp]) {
			try {
				wTables[tmp].put(put);
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	/**
	 * Force to flush all HTable objects to write all the left data to HBase.
	 * NOTICE: MUST call this function before program exits!
	 */
	public void flushAllTables() {
		for (int k = 0; k < tableN; k++) {
			synchronized (wTables[k]) {
				try {
					wTables[k].flushCommits();
				} catch (IOException e) {
					LOG.error("flush to hbase exception: " + e);
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Return HBase table name
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}
	
	/**
	 * Return HBase table column family
	 * @return
	 */
	public String getColumnFamily() {
		return Bytes.toString(columnFamily);
	} 
	
	/**
	 * Return HBase table column
	 * @return
	 */
	public String getColumn() {
		return Bytes.toString(column);
	} 
	
	/**
	 * Initialize HTable object for writing data to HBase
	 * 
	 * @return
	 */
	private boolean initHTables() {
		boolean b = true;
		try {
			wTables = new HTable[tableN];
			for (int i = 0; i < tableN; i++) {
				wTables[i] = new HTable(conf, tableName);
				wTables[i].setWriteBufferSize(1024 * 1024);
				wTables[i].setAutoFlush(false);
			}
		} catch (IOException e) {
			LOG.error("Catch exception: ", e);
			b = false;
		}
		return b;
	}

	/**
	 * Startup daemon thread to flush data to HBase every second
	 */
	private void startDaemon() {
		for (int i = 0; i < tableN; i++) {
			final int tmp = i;
			Thread th = new Thread() {
				@Override
				public void run() {
					while (true) {
						try {
							sleep(1000);
						} catch (InterruptedException e) {
							LOG.error(e);
							e.printStackTrace();
						}
						synchronized (wTables[tmp]) {
							try {
								wTables[tmp].flushCommits();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			};
			th.setDaemon(true);
			th.start();
		}
	}
	
	/**
	 * Generate Put object
	 * 
	 * @param rowkey
	 * @param value
	 * @return
	 */
	private Put getPutForTable(final byte[] rowkey, final byte[] value) {
		Put put = new Put(rowkey);
		put.add(columnFamily, column, value);
		return put;
	}
}
