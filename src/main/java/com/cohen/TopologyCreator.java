package com.cohen;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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
    public static void main(String[] args) throws Exception {
        boolean local = true;
        if (local) {
            local();
        } else {
            remote();
        }
    }

    private static void local() throws InterruptedException {
        TridentTopology tridentTopology = new TridentTopology();
        Stream kafkaStream = tridentTopology.newStream("kafka-stream", new KafkaSpout());
        Stream groupStream = kafkaStream.each(new Fields("batchId", "billJson"), new BillPredealFunction(), new Fields("bill"));
        List<Stream> aggregateStreams = new ArrayList<Stream>();
        aggregateStreams.add(groupStream.partitionBy(new Fields("bill")).partitionAggregate(new Fields("bill"), new NamtAggregator(), new Fields("key", "value")));
        aggregateStreams.add(groupStream.partitionBy(new Fields("bill")).partitionAggregate(new Fields("bill"), new NjamtAggregator(), new Fields("key", "value")));
        tridentTopology.merge(aggregateStreams)
                .partitionBy(new Fields("key"))
                .partitionAggregate(new Fields("key", "value"), new SumAggregator(), new Fields("sum"));
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setNumWorkers(1);
        //设置storm的超时时间大于设置的超时时间
        conf.setMessageTimeoutSecs(70);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2);
        cluster.submitTopology("tridentTopology", conf, tridentTopology.build());
    }

    private static void remote() throws Exception {
//        Map conf = new HashMap();
//        //topology所有自定义的配置均放入这个Map
//        TopologyBuilder builder = new TopologyBuilder();
//        //创建topology的生成器
//        int spoutParal = 1;
//        //获取spout的并发设置
//        SpoutDeclarer spout = builder.setSpout("out-word", new WordSpout(), spoutParal);
//        //创建Spout， 其中new SequenceSpout() 为真正spout对象，SequenceTopologyDef.SEQUENCE_SPOUT_NAME 为spout的名字，注意名字中不要含有空格
//        int boltParal = 1;
//        //获取bolt的并发设置
//        BoltDeclarer totalBolt = builder.setBolt("word-count", new CountBolt(), boltParal).shuffleGrouping("out-word");
//        //创建bolt， SequenceTopologyDef.TOTAL_BOLT_NAME 为bolt名字，TotalCount 为bolt对象，boltParal为bolt并发数，
//        // shuffleGrouping（SequenceTopologyDef.SEQUENCE_SPOUT_NAME），
//        // 表示接收SequenceTopologyDef.SEQUENCE_SPOUT_NAME的数据，并且以shuffle方式，
//        // 即每个spout随机轮询发送tuple到下一级bolt中
//        int ackerParal = 1;
//        Config.setNumAckers(conf, ackerParal);
//        //设置表示acker的并发数
//        int workerNum = 2;
//        conf.put(Config.TOPOLOGY_WORKERS, workerNum);
//        //表示整个topology将使用几个worker
//        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
//        //设置topolog模式为分布式，这样topology就可以放到JStorm集群上运行
//        StormSubmitter.submitTopology("streamName", conf, builder.createTopology());
//        //提交topology
    }
}