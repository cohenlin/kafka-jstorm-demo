package com.cohen.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSONObject;
import com.cohen.common.KeyedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 林金成
 * @date 2018/9/29 16:22
 */
public class KafkaSpout implements IBatchSpout {
    /**
     * log4j日志工具
     */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    /**
     * spout最大等待时间
     */
    private static final Integer TOPOLOGY_MESSAGE_TIMEOUT_SECS = 60;
    /**
     * 一批（最大任务数量为30）执行所需最大时间
     */
    private static final Integer BATCH_MAX_DURING_TIME_MILLIS = 3000;
    /**
     * 一批任务最大数量
     */
    private static final Integer BATCH_MAX_SIZE = 30;
    /**
     * 任务队列
     */
    private KeyedQueue<JSONObject> taskQueue;
    /**
     * 批次与批次中所有的订单号的映射，为了结束批次后发送日志给监控服务所设置的
     */
    private Map<String, List<String>> historyMap = new HashMap<>();

    @Override
    public void open(Map conf, TopologyContext context) {
        LOG.debug("-------------------------KafkaSpout.open-------------------------");
        this.taskQueue = new KeyedQueue<>();
        Thread thread = new Thread(() -> {
            new KafkaListener(this.taskQueue).start();
        });
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * 保证在满足一下三个条件的前提下，提交最大数量的任务
     * 1.在spout最大等待时间内
     * 2.在一批最大数量的执行所需总时间内
     * 3.本次提交任务总数小于一批最大数量
     */
    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        LOG.debug("-------------------------KafkaSpout.emitBatch-------------------------");
        try {
            // 本次操作执行开始时间
            long start = System.currentTimeMillis();
            int i = 0;
            String batchidStr = String.valueOf(batchId);
            while (((System.currentTimeMillis() - start) <= TOPOLOGY_MESSAGE_TIMEOUT_SECS * 1000)
                    && ((System.currentTimeMillis() - start) <= BATCH_MAX_DURING_TIME_MILLIS)
                    && ((i == 0) || i < BATCH_MAX_SIZE)) {
                try {
                    if (this.taskQueue.hasTask()) {
                        //将接收队列中的订单逐一的拿出来，并发送给后续的bolt，实际只有excute执行完毕，才会整体发送
                        JSONObject bill = this.taskQueue.take();
                        if (!this.historyMap.containsKey(batchidStr)) {
                            this.historyMap.put(batchidStr, new ArrayList());
                        }
                        String billLog = "tenantId: ".concat(bill.getString("vgroupcode"))
                                .concat(", vscode: ").concat(bill.getString("vscode"))
                                .concat(", dworkdate: ").concat(bill.getString("dworkdate"))
                                .concat(", vbcode: ").concat(bill.getString("vbcode"))
                                .concat(", vorclass: ").concat(bill.getString("vorclass"));
                        this.historyMap.get(batchidStr).add(billLog);
                        collector.emit(new Values(batchidStr, bill));
                        i++;
                    } else {
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (i == 0) {
                collector.emit(new Values(batchidStr, new JSONObject()));
            }
            LOG.info("批次：" + batchId + "， 数量：" + i);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量处理完成之后会调用此方法
     *
     * @param batchId : 批次号
     */
    @Override
    public void ack(long batchId) {
        LOG.debug("-------------------------KafkaSpout.ack-------------------------");
        // TODO 批量处理完成后，发送批次处理日志
        String id = String.valueOf(batchId);
        if (historyMap != null && historyMap.containsKey(id)) {
            for (String log : historyMap.get(id)) {
                LOG.info("log: " + log);
            }
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("batchId", "billJson");
    }
}