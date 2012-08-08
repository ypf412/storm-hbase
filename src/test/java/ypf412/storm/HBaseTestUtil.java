package ypf412.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ypf412.storm.util.Constants;
import ypf412.storm.util.PropConfig;

public class HBaseTestUtil {
	
	/**
	 * Create HBase local cluster
	 * 
	 * @return
	 * @throws Exception
	 */
	public static HBaseTestingUtility createLocalHBaseCluster()
			throws Exception {
		HBaseTestingUtility hbaseUtil = new HBaseTestingUtility();
		hbaseUtil.startMiniCluster();
		return hbaseUtil;
	}

	/**
	 * Shutdown HBase local cluster
	 * 
	 * @param hbaseUtil
	 * @throws Exception
	 */
	public static void shutdownLocalHBaseCluster(HBaseTestingUtility hbaseUtil)
			throws Exception {
		hbaseUtil.cleanupTestDir();
		hbaseUtil.shutdownMiniCluster();
	}

	/**
	 * Write configuration of HBase local cluster to hbase-site.xml
	 * 
	 * @param hbaseUtil
	 * @throws Exception
	 */
	public static void writeLocalHBaseXml(HBaseTestingUtility hbaseUtil)
			throws Exception {
		String path = ClassLoader.getSystemResource("hbase-site.xml").getFile();
		OutputStream os = new FileOutputStream(path);
		hbaseUtil.getConfiguration().writeXml(os);
		os.close();
	}
	
	/**
	 * Load stream data to HBase table
	 * @param filePath
	 * @param hTable
	 * @param hbasePropConfig
	 */
	public static void loadStreamDataToHBase(String filePath, HTable hTable, PropConfig hbasePropConfig) {
		final String CTRL_C = "";
		final String CTRL_E = "";
		List<Put> putList = new ArrayList<Put>();
		if (!filePath.startsWith("/")) {
			filePath = ClassLoader.getSystemResource(filePath).getFile();
		}
		try {
			FileReader fr = new FileReader(filePath);
			BufferedReader br = new BufferedReader(fr);
			try {
				String line = null;
				while ((line = br.readLine()) != null) {
					String[] kvs = line.split(CTRL_C);
					if (kvs.length != 2) // must be two key-value parts
						continue;
					String key = kvs[0];
					String value = kvs[1];
					String[] keys = key.split(CTRL_E);
					if (keys.length < 2) // at least contains <shardingkey, timestamp>
						continue;
					short sharding = Short.parseShort(keys[0]);
					int timestamp = Integer.parseInt(keys[1]);
					byte[] shardingKey = {(byte)sharding};
					byte[] timestampKey = Bytes.toBytes(timestamp);
					byte[] rowKey = Bytes.add(shardingKey, timestampKey);
					for (int i=2; i<keys.length; i++) {
						rowKey = Bytes.add(rowKey, Bytes.toBytes(keys[i]));
					}
					Put put = new Put(rowKey);
					byte[] columnFamily = Bytes.toBytes(Constants.HBASE_DEFAULT_COLUMN_FAMILY);
					String columnFamilyStr = hbasePropConfig.getProperty("hbase.table.column_family");
					if (columnFamilyStr != null && !columnFamilyStr.equals(""))
						columnFamily = Bytes.toBytes(columnFamilyStr);
					byte[] column = Bytes.toBytes(Constants.HBASE_DEFAULT_COLUMN);
					String columnStr = hbasePropConfig.getProperty("hbase.table.column");
					if (columnStr != null && !columnStr.equals(""))
						column = Bytes.toBytes(columnStr);
					put.add(columnFamily, column, Bytes.toBytes(value));
					putList.add(put);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		try {
			hTable.put(putList);
			hTable.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
