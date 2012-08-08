package ypf412.storm;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.KillOptions;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import ypf412.storm.util.StreamData;

/**
 * This StormTestUtil is based on the local Storm test suit provided by Wang Xiaozhe,
 * and add the static classes StormTestSpout and StormTestBolt for my test case.
 *
 */
public class StormTestUtil {
	public static final String TEST_TOPO_NAME = "test";

	LocalCluster cluster;

	Config conf;

	public StormTestUtil(Config conf, LocalCluster cluster) {
		this.conf = conf;
		this.cluster = cluster;
	}

	/**
	 * Get configuration of Storm cluster
	 * 
	 * @return
	 */
	public Config getConfig() {
		return conf;
	}

	/**
	 * Submit and run Storm test topology
	 * @param builder
	 * @throws Exception
	 */
	public void submitTopology(TopologyBuilder builder) throws Exception {
		submitTopology(builder, 1000L);
	}

	/**
	 * Submit and run Storm test topology
	 * @param builder
	 * @param millis
	 */
	public void submitTopology(TopologyBuilder builder, long millis)
			throws Exception {
		cluster.submitTopology(TEST_TOPO_NAME, conf, builder.createTopology());

		// Sleep some time to make Storm cluster run topology
		Utils.sleep(millis);

		// MUST kill Storm topology at last
		killToplogy();
	}

	/**
	 * Kill Storm test topology
	 * 
	 * @throws Exception
	 */
	private void killToplogy() {
		KillOptions opt = new KillOptions();
		opt.set_wait_secs(2);
		cluster.killTopologyWithOpts(TEST_TOPO_NAME, opt);

		Utils.sleep(3000);
	}

	/**
	 * Create Storm local cluster
	 * 
	 * @return
	 * @throws Exception
	 */
	public static StormTestUtil createLocalStormCluster() throws Exception {
		Config conf = new Config();
		return createLocalStormCluster(conf);
	}

	/**
	 * Create Storm local cluster
	 * 
	 * @param conf
	 * @return
	 * @throws Exception
	 */
	public static StormTestUtil createLocalStormCluster(Config conf)
			throws Exception {
		conf.setDebug(true);
		conf.setMaxTaskParallelism(4);

		LocalCluster cluster = new LocalCluster();
		StormTestUtil info = new StormTestUtil(conf, cluster);
		return info;
	}

	/**
	 * Shutdown Storm local cluster
	 * 
	 * @param util
	 * @throws Exception
	 */
	public static void shutdownLocalStormCluster(StormTestUtil util)
			throws Exception {
		util.cluster.shutdown();
	}

	/**
	 * Load tuples from file path
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static List<Object[]> loadTuples(String path) throws Exception {
		final String CTRL_C = "";
		final String CTRL_E = "";
		if (!path.startsWith("/")) {
			path = ClassLoader.getSystemResource(path).getFile();
		}
		
		List<Object[]> tupleList = new ArrayList<Object[]>();
		try {
			FileReader fr = new FileReader(path);
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
					int i = 2;
					while(i < keys.length) {
						rowKey = Bytes.add(rowKey, Bytes.toBytes(keys[i]));
						i++;
					}
					Object[] tuple = new Object[] {new Short(sharding), new StreamData(rowKey), new StreamData(Bytes.toBytes(value))};
					tupleList.add(tuple);		
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		return tupleList;
	}

	/**
	 * Storm Spout for testing
	 * 
	 */
	public static class StormTestSpout extends BaseRichSpout {

		private static final long serialVersionUID = 6526425905460276786L;

		String[] fieldNames;
		int delay;
		ConcurrentLinkedQueue<Object[]> data = new ConcurrentLinkedQueue<Object[]>();
		SpoutOutputCollector collector;

		public StormTestSpout(String[] fields) {
			this(fields, 10);
		}

		public StormTestSpout(String[] fields, int delayInMillis) {
			fieldNames = fields;
			delay = delayInMillis;
		}

		public void feedData(Object[]... tuples) {
			for (Object[] tuple : tuples) {
				data.add(tuple);
			}
		}

		@Override
		public void open(@SuppressWarnings("rawtypes") Map conf,
				TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void nextTuple() {
			Utils.sleep(delay);
			Object[] vals = data.poll();
			if (vals != null) {
				collector.emit(new Values(vals));
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields(fieldNames));
		}

	}

	/**
	 * Storm Bolt for testing
	 * 
	 */
	public static class StormTestBolt extends BaseRichBolt {

		private static final long serialVersionUID = 2996268980974990545L;

		private int[] index = new int[Short.MAX_VALUE];
		
		private Map<Short, List<Object[]>> data = new HashMap<Short, List<Object[]>>();

		public StormTestBolt() {
			for (int i=0; i<Short.MAX_VALUE; i++)
				index[i] = 0;
		}

		public void expectSeq(Object[]... exps) {
			for (Object[] exp : exps) {
				Short sharding = (Short)exp[0];
				List<Object[]> list = null;
				if (data.containsKey(sharding)) {
					list = data.get(sharding);
					list.add(exp);
				} else {
					list = new ArrayList<Object[]>();
					list.add(exp);
				}
				data.put(sharding, list);
			}
		}

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
				TopologyContext context, OutputCollector collector) {
		}

		@Override
		public void execute(Tuple input) {
			List<Object> fields = input.getValues();
			Short sharding = (Short)fields.get(0);

			List<Object[]> list = data.get(sharding);
			int idx = index[sharding] % list.size();
			index[sharding]++;
			Object[] exp = list.get(idx);
			assertEquals(exp.length, input.size());

			for (int i = 0; i < exp.length; i++) {
				if (exp[i].getClass() == byte[].class) { // deal with byte array object
					assertEquals(byte[].class, input.nth(i).getClass());
					assertTrue(Bytes.compareTo((byte[]) exp[i],
							(byte[]) input.nth(i)) == 0);
				} else if (exp[i].getClass() == StreamData.class) {
					byte[] expBytes = ((StreamData)exp[i]).getData(); // deal with StreamData object
					byte[] tupBytes = ((StreamData)input.nth(i)).getData();
					assertTrue(Bytes.compareTo(expBytes, tupBytes) == 0);
				} else { // other type object
					assertEquals(exp[i], input.nth(i));
				}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}

	}
	
}
