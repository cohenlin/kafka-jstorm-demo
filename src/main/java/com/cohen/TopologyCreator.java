package com.cohen;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import com.cohen.function.BillPredealFunction;
import com.cohen.function.NamtAggregator;
import com.cohen.function.NjamtAggregator;
import com.cohen.function.SumAggregator;
import com.cohen.spout.KafkaSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 林金成
 * @date 2018/9/29 10:45
 */
public class TopologyCreator {
    private static final String TOPOLOGY_NAME = "tridentTopology";

    public static void main(String[] args) throws Exception {
        boolean local = true;
        TridentTopology tridentTopology = new TridentTopology();
        Stream kafkaStream = tridentTopology.newStream("kafka-stream", new KafkaSpout());
        Stream groupStream = kafkaStream.each(new Fields("batchId", "billJson"), new BillPredealFunction(), new Fields("bill"));
        List<Stream> aggregateStreams = new ArrayList<Stream>();
        aggregateStreams.add(groupStream.partitionBy(new Fields("bill")).partitionAggregate(new Fields("bill"), new NamtAggregator(), new Fields("key", "value")));
        aggregateStreams.add(groupStream.partitionBy(new Fields("bill")).partitionAggregate(new Fields("bill"), new NjamtAggregator(), new Fields("key", "value")));
        tridentTopology.merge(aggregateStreams)
                .partitionBy(new Fields("key"))
                .partitionAggregate(new Fields("key", "value"), new SumAggregator(), new Fields("sum"));
        Config conf = new Config();
        conf.setNumWorkers(1);
        //设置storm的超时时间大于设置的超时时间
        conf.setMessageTimeoutSecs(70);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2);
        if (local) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, conf, tridentTopology.build());
        } else {
            StormSubmitter.setLocalNimbus(null);
            StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, tridentTopology.build());
        }
    }
}