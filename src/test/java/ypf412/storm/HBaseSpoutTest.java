package ypf412.storm;


import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ypf412.storm.StormTestUtil.StormTestBolt;
import ypf412.storm.spout.HBaseSpout;
import ypf412.storm.util.Constants;
import ypf412.storm.util.PropConfig;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class HBaseSpoutTest {

	static StormTestUtil stormUtil;
	static HBaseTestingUtility hbaseUtil;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// make sure Storm cluster is started before HBase cluster
		stormUtil = StormTestUtil.createLocalStormCluster();
		hbaseUtil = HBaseTestUtil.createLocalHBaseCluster();

		// mock current time to a fixed time
		DateTimeUtils.setCurrentMillisFixed(new DateTime(2012, 8, 7, 12, 0, 0,
				0).getMillis());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		StormTestUtil.shutdownLocalStormCluster(stormUtil);
		HBaseTestUtil.shutdownLocalHBaseCluster(hbaseUtil);
	}

	@Test
	public void testHBaseSpout() throws Exception {
		hbaseUtil.cleanupTestDir();
		HBaseTestUtil.writeLocalHBaseXml(hbaseUtil);
		
		PropConfig hbasePropConfig;
		try {
			hbasePropConfig = new PropConfig("hbase.properties");
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		
		String tableName = Constants.HBASE_DEFAULT_TABLE_NAME;
		String tableNameStr = hbasePropConfig.getProperty("hbase.table.name");
		if (tableNameStr != null && !tableNameStr.equals(""))
			tableName = tableNameStr;
		String columnFamily = Constants.HBASE_DEFAULT_COLUMN_FAMILY;
		String columnFamilyStr = hbasePropConfig.getProperty("hbase.table.column_family");
		if (columnFamilyStr != null && !columnFamilyStr.equals(""))
			columnFamily = columnFamilyStr;

		HTable htable = hbaseUtil.createTable(Bytes.toBytes(tableName), Bytes.toBytes(columnFamily));
		HBaseTestUtil.loadStreamDataToHBase(
				ClassLoader.getSystemResource("datasource.txt")
						.getPath(), htable, hbasePropConfig);
		int count = hbaseUtil.countRows(htable);
		assertTrue(count > 0);
		System.out.println("*** load " + count + " rows into hbase test table: " + tableName);

		stormUtil.getConfig().put(Constants.STORM_PROP_CONF_FILE,
				"storm.properties");
		stormUtil.getConfig().put(Constants.HBASE_PROP_CONF_FILE,
				"hbase.properties");
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("hbaseSpout", new HBaseSpout());
		StormTestBolt sinkBolt = new StormTestBolt();
		List<Object[]> tuples = StormTestUtil
				.loadTuples("datasource.txt");
		for (Object[] tuple : tuples) {
			sinkBolt.expectSeq(tuple);
		}
		builder.setBolt("sinkBolt", sinkBolt).fieldsGrouping("hbaseSpout",
				new Fields("sharding"));
		stormUtil.submitTopology(builder, 5000);
		hbaseUtil.deleteTable(Bytes.toBytes(tableName));
	}
	
}
