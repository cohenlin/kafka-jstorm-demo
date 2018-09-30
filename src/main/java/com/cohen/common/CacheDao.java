package com.cohen.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 林金成
 * @date 2018/9/29 18:27
 */
public class CacheDao {
    /**
     * log4j日志工具
     */
    private static final Logger LOG = LoggerFactory.getLogger(CacheDao.class);
    private Map<String, Double> map;

    public synchronized void set(String key, Double value) {
        if (map == null) {
            map = new HashMap<>();
        }
        map.put(key, map.getOrDefault(key, 0.0D) + value);
    }

    public synchronized void print() {
        if (map != null) {
            for (Map.Entry<String, Double> entry : map.entrySet()) {
                LOG.info(entry.getKey() + " : " + entry.getValue());
            }
        }
    }

    public static CacheDao getInstance() {
        return CacheDaoHolder.instance;
    }

    private static class CacheDaoHolder {
        private static CacheDao instance = new CacheDao();
    }
}