package ca.dealsaccess.holt.storm.spout;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class LogSpout extends BaseRichSpout {

    private Jedis jedis;
    private String host;
    private int port;
    private SpoutOutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url"));
    }

    @SuppressWarnings("rawtypes")
	@Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        connectToRedis();
    }

    private void connectToRedis() {
        jedis = new Jedis(host, port);
    }

    @Override
    public void nextTuple() {
        String url = jedis.rpop("url");
        if(url==null) {
            try { Thread.sleep(50); } catch (InterruptedException e) {}
        } else {
            collector.emit(new Values(url));
        }
    }
    
}
