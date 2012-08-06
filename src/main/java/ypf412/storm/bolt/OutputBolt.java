package ypf412.storm.bolt;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import ypf412.storm.util.StreamData;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * This bolt just output the received tuples.
 *  
 * @author jiuling.ypf
 *
 */
public class OutputBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = -8622707506877979035L;

	public OutputBolt() {
		
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
	}
	
	@Override
	public void execute(Tuple input) {
		List<Object> fields = input.getValues();
		System.out.println("recv tuple: " + input + ", field num: " + fields.size());
		for (int i=0; i<fields.size(); i++) {
			StreamData streamData = (StreamData)fields.get(i);
			System.out.println("tuple field " + i + ": " + Bytes.toString(streamData.getData()));
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}