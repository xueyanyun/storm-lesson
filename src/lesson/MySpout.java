package lesson;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private BufferedReader br;
	private SpoutOutputCollector collector;


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;		
		
		try {
			br = new BufferedReader(new FileReader("track.log"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void close() {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

	@Override
	public void activate() {
		
	}

	@Override
	public void deactivate() {
		
	}

	@Override
	public void nextTuple() {
		String line;
		try {
			while ((line = br.readLine()) != null) {
				collector.emit(new Values(line), line);				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void ack(Object msgId) {
		//System.out.println("OK: " + msgId);
	}

	@Override
	public void fail(Object msgId) {
		//System.out.println("FAIL: " + msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
