package ypf412.storm.topology;

import ypf412.storm.bolt.DumpToHBaseBolt;
import ypf412.storm.spout.HBaseSpout;
import ypf412.storm.util.Constants;
import ypf412.storm.util.PropConfig;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * This is a simple storm topology for testing HBaseSpout.
 * It consists a DumpToHBaseBolt which dump the tuples received from HBaseSpout to another HBase table.
 * 
 * @author jiuling.ypf
 *
 */
public class DumpToHBaseTopology {
	
	/**
	 * HBase Data Dump to Another HBase Table Topology
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		PropConfig pc = new PropConfig("storm.properties");
		int topoWorkers = Integer.valueOf(pc.getProperty("storm.topolopy.workers"));
		
		int spoutTasks = Integer.valueOf(pc
				.getProperty("storm.spout.tasks"));
		builder.setSpout("hbaseSpout", new HBaseSpout(), spoutTasks);
		
		int boltTasks = spoutTasks;
		builder.setBolt("dumpBolt", new DumpToHBaseBolt(), boltTasks)
				.fieldsGrouping("hbaseSpout", new Fields("sharding"));

		Config conf = new Config();
		conf.put(Constants.STORM_PROP_CONF_FILE,
				"storm.properties");
		conf.put(Constants.HBASE_PROP_CONF_FILE,
				"hbase.properties");
		
		if (args != null && args.length > 0) { // run on storm cluster
			conf.setNumAckers(1);
			conf.setNumWorkers(topoWorkers);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else { // run on local cluster
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

}
