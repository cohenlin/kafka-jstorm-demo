package com.cohen.function;

import com.cohen.common.CacheDao;
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
 * @date 2018/9/29 18:10
 */
public class SumAggregator implements Aggregator<Map<String, Object>> {
    /**
     * log4j日志工具
     */
    private static final Logger LOG = LoggerFactory.getLogger(SumAggregator.class);

    @Override
    public Map<String, Object> init(Object batchId, TridentCollector collector) {
        LOG.debug("-------------------------SumAggregator.init-------------------------");
        return new HashMap<>();
    }

    @Override
    public void aggregate(Map<String, Object> val, TridentTuple tuple, TridentCollector collector) {
        LOG.debug("-------------------------SumAggregator.aggregate-------------------------");
        val.put(((String) tuple.getValueByField("key")), tuple.getValueByField("value"));
    }

    @Override
    public void complete(Map<String, Object> val, TridentCollector collector) {
        LOG.debug("-------------------------SumAggregator.complete-------------------------");
        if (val.size() > 0) {
            for (Map.Entry<String, Object> entry : val.entrySet()) {
                CacheDao.getInstance().set(entry.getKey(), (Double) entry.getValue());
            }
        }
        CacheDao.getInstance().print();
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
    }

    @Override
    public void cleanup() {
    }
}