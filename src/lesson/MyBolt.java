package lesson;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MyBolt implements IRichBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private static int num;
	private int taskId;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
		this.taskId = context.getThisTaskId();
		
//		Map<Integer, String> taskToComponent = context.getTaskToComponent();
//		Iterator<Entry<Integer, String>> iterator = taskToComponent.entrySet().iterator();
//		while (iterator.hasNext()) {
//			Entry<Integer, String> next = iterator.next();
//			System.err.println(next.getKey());
//			System.err.println(next.getValue());
//			
//		}
//		
	}

	@Override
	public void execute(Tuple input) {
		try {
			String log = input.getStringByField("log");
			if (log != null) {
				synchronized (MyBolt.class) {
					num ++;
				}
				
				System.err.println(Thread.currentThread().getName() + " taskid:" + taskId +  "  lines:" + num + "   session_id:"+ log.split("\t")[1]);
				
			}
			collector.ack(input);
		} catch (Exception e) {
			collector.fail(input);
		}
		
		
		
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
