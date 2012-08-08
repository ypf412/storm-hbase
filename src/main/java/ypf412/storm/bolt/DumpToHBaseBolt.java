package ypf412.storm.bolt;

import java.util.List;
import java.util.Map;

import ypf412.storm.util.HBaseDumper;
import ypf412.storm.util.StreamData;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class DumpToHBaseBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 7642830621960029476L;
	
	private HBaseDumper hbaseDumper;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		hbaseDumper = new HBaseDumper("result", "dcf", "dc");

	}

	@Override
	public void execute(Tuple input) {
		List<Object> fields = input.getValues();
		if (fields.size() == 3) {
			StreamData rowKey = (StreamData)fields.get(1); //key field
			StreamData value = (StreamData)fields.get(2); //value field
			hbaseDumper.writeToTable(rowKey.getData(), value.getData());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
