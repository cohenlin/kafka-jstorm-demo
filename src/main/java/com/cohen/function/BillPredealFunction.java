package com.cohen.function;

import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSONObject;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * @author 林金成
 * @date 2018/9/309:18
 */
public class BillPredealFunction implements Function {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        JSONObject bill = (JSONObject) tuple.getValueByField("billJson");
        collector.emit(new Values(bill));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
    }

    @Override
    public void cleanup() {
    }
}