package ypf412.storm.util;

/**
 * This is a Constants class which holds the common default variables of storm-hbase project.
 * These variables can be overrided by storm.properties file or hbase.properties file.
 * 
 * @author jiuling.ypf
 *
 */
public interface Constants {
	
	public static final String STORM_PROP_CONF_FILE = "storm_prop_path";
	
	public static final String HBASE_PROP_CONF_FILE = "hbase_prop_path";
	
	public static final int STORM_SPOUT_DEFAULT_QUEUE_SIZE = 1000;
	
	public static final int STORM_SPOUT_DEFAULT_START_TIMESTAMP = 0;
	
	public static final int STORM_SPOUT_DEFAULT_STOP_TIMESTAMP = 0;
	
	public static final short HBASE_DEFAULT_SHARDING_NUM = 32;

	public static final String HBASE_DEFAULT_TABLE_NAME = "storm-hbase";

	public static final String HBASE_DEFAULT_COLUMN_FAMILY = "cf";

	public static final String HBASE_DEFAULT_COLUMN = "c";

	public static final int HBASE_DEFAULT_CACHING_RECORD = 100;
	
	public static final int HBASE_STREAM_DATA_START_SEC = 3 * 60;
	
	public static final int HBASE_STREAM_DATA_STOP_SEC = 0;

}
