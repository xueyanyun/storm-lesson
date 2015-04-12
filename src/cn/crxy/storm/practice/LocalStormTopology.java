package cn.crxy.storm.practice;

import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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

/**
 * 作业：实现单词计数�?
 *     (1)要求从一个文件夹中把�?��文件都读取，计算�?��文件中的单词出现次数�? *     (2)当文件夹中的文件数量增加是，实时计算�?��文件中的单词出现次数�? */
public class LocalStormTopology 
{
	
	public static class DataSourceSpout extends BaseRichSpout{
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;
		
		final Random random = new Random();
		/**
		 * 在本实例运行时，首先被调�?		 */
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}
		
		/**
		 * 认为heartbeat，永无休息，死循环的调用。线程安全的操作�?		 */
		int i = 0;
		public void nextTuple() {
			System.err.println("Spout  "+ i);
			//送出去，送个bolt
			//Values是一个value的List
			this.collector.emit(new Values(i++));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			//Fields是一个field的List
			declarer.declare(new Fields("v1"));
		}
	}
	
	public static class SumBolt extends BaseRichBolt{
		private Map conf;
		private TopologyContext context;
		private OutputCollector collector;
		
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}
		
		/**
		 * 死循环，用于接收bolt送来的数�?		 */
		int sum = 0;
		public void execute(Tuple tuple) {
			final Integer value = tuple.getIntegerByField("v1");
			sum += value;
			System.err.println(sum);
		}


		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			
		}

	}
	
    public static void main( String[] args ) throws InterruptedException
    {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", new DataSourceSpout());
        builder.setBolt("2", new SumBolt()).shuffleGrouping("1");
        
        final LocalCluster localCluster = new LocalCluster();
        final Config config = new Config();
		localCluster.submitTopology(LocalStormTopology.class.getSimpleName(), config, builder.createTopology());
		Thread.sleep(9999999);
		localCluster.shutdown();
    }
}
