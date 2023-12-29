package com.aarengee;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.SummaryMetricFamily;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ConnectionPool;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static java.util.Collections.singletonList;

public class RedisCollector extends Collector {

  private static final List<String> LABEL_NAMES = singletonList("host");

  private Map<String, ConnectionPool> nodePools = new ConcurrentHashMap<>();

  private static RedisCollector redisCollector;

  private RedisCollector() {}

  public static synchronized RedisCollector getCollector() {
    if (redisCollector == null) {
      redisCollector = new RedisCollector().register();
    }
    return redisCollector;
  }

  @Override
  public List<MetricFamilySamples> collect() {
    return Arrays.asList(
        gauge("jedis_pool_num_active", "Jedis Pool Active connections", ConnectionPool::getNumActive),
        gauge("jedis_pool_idle_connections", "Jedis Pool Idle connections", ConnectionPool::getNumIdle),
        gauge("jedis_pool_num_waiters", "Jedis Pool Waiting connections", ConnectionPool::getNumWaiters),
        summary("jedis_pool_mean_borrow_wait_time_millis", "Jedis Pool Mean Borrow Wait Time in millis",
            p -> p.getMeanBorrowWaitDuration().toMillis()),
        summary("jedis_pool_max_borrow_wait_time_millis", "Jedis Pool Max Borrow Wait Time in millis",
            p -> p.getMaxBorrowWaitDuration().toMillis()));
  }

  public void track(JedisCluster cluster) {
    nodePools = cluster.getClusterNodes();
  }

  private GaugeMetricFamily gauge(String metric, String help,
                                  ToIntFunction<ConnectionPool> driverFunction) {
    GaugeMetricFamily metricFamily = new GaugeMetricFamily(metric, help, LABEL_NAMES);
    nodePools.forEach((poolName, pool) -> metricFamily.addMetric(singletonList(poolName), driverFunction.applyAsInt(pool)));
    return metricFamily;
  }

  private SummaryMetricFamily summary(String metric, String help, Function<ConnectionPool,Long> driverFunction) {
    SummaryMetricFamily metricFamily = new SummaryMetricFamily(metric, help, LABEL_NAMES);
    nodePools.forEach((poolName, pool) -> metricFamily.addMetric(singletonList(poolName), driverFunction.apply(pool), 0));
    return metricFamily;
  }
}
