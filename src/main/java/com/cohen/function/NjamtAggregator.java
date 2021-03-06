package com.cohen.function;

import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 林金成
 * @date 2018/9/29 18:03
 */
public class NjamtAggregator implements Aggregator<Map<String, Object>> {
    /**
     * log4j日志工具
     */
    private static final Logger LOG = LoggerFactory.getLogger(NjamtAggregator.class);

    @Override
    public Map<String, Object> init(Object batchId, TridentCollector collector) {
        LOG.debug("-------------------------NjamtAggregator.init1-------------------------");
        return new HashMap<>();
    }

    @Override
    public void aggregate(Map<String, Object> val, TridentTuple tuple, TridentCollector collector) {
        LOG.debug("-------------------------NjamtAggregator.aggregate-------------------------");
        JSONObject bill = (JSONObject) tuple.getValueByField("bill");
        val.put("njamt", (Double) val.getOrDefault("namt", 0.0D) + Double.parseDouble(String.valueOf(bill.getOrDefault("njamt", 0.0D))));
    }

    @Override
    public void complete(Map<String, Object> val, TridentCollector collector) {
        LOG.debug("-------------------------NjamtAggregator.complete-------------------------");
        if (val != null && val.size() > 0) {
            for (Map.Entry<String, Object> entry : val.entrySet()) {
                collector.emit(new Values(entry.getKey(), entry.getValue()));
            }
        }
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
    }

    @Override
    public void cleanup() {
    }
}