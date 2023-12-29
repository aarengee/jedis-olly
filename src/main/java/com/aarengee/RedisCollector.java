package com.aarengee;


import io.prometheus.metrics.model.registry.Collector;
import io.prometheus.metrics.model.snapshots.ClassicHistogramBuckets;
import io.prometheus.metrics.model.snapshots.Exemplars;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.Label;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricMetadata;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import static java.util.Collections.singletonList;

public class RedisCollector implements Collector {

  private static final String LABEL_HOST = "host";

  private Map<String, ConnectionPool> poolStatsMap = new ConcurrentHashMap<>();

  private static RedisCollector redisCollector;

  private RedisCollector() {
  }

  public static synchronized RedisCollector getCollector() {
    if (redisCollector == null) {
      redisCollector = new RedisCollector();
    }
    return redisCollector;
  }

  @Override
  public MetricSnapshot collect() {
    return Arrays.asList(
        createGauge(meta("jedis_pool_num_active", "Jedis Pool Active connections"), ConnectionPool::getNumActive),
        createGauge(meta("jedis_pool_idle_connections", "Jedis Pool Idle connections"), ConnectionPool::getNumIdle),
        createGauge(meta("jedis_pool_num_waiters", "Jedis Pool Waiting connections"), ConnectionPool::getNumWaiters),
        createSummary(meta("jedis_pool_mean_borrow_wait_time_millis", "Jedis Pool Mean Borrow Wait Time in millis"), ConnectionPool::getMeanBorrowWaitTimeMillis),
        createSummary(meta("jedis_pool_max_borrow_wait_time_millis", "Jedis Pool Max Borrow Wait Time in millis"), ConnectionPool::getMaxBorrowWaitTimeMillis)
    );
  }

  public void track(JedisCluster cluster) {
    poolStatsMap = cluster.getClusterNodes();
  }

  private MetricMetadata meta(String promName, String help) {
    return new MetricMetadata(promName, help);
  }

  private GaugeSnapshot createGauge(MetricMetadata metadata,
                                    ToIntFunction<ConnectionPool> driverFunction) {
    List<GaugeSnapshot.GaugeDataPointSnapshot> data = new ArrayList<>();
    poolStatsMap.forEach((poolName, pool) ->
        data.add(GaugeSnapshot.GaugeDataPointSnapshot.builder()
            .labels(Labels.of(LABEL_HOST, poolName))
            .value(driverFunction.applyAsInt(pool))
            .build())
    );
    return new GaugeSnapshot(metadata, data);
  }

  private SummarySnapshot createSummary(MetricMetadata metadata, ToLongFunction<ConnectionPool> driverFunction) {
    List<SummarySnapshot.SummaryDataPointSnapshot> data = new ArrayList<>();
        public HistogramDataPointSnapshot(
        ClassicHistogramBuckets classicBuckets,
    double sum,
    Labels labels,
    Exemplars exemplars,
    long createdTimestampMillis)
    poolStatsMap.forEach((poolName, pool) ->
        data.add(SummarySnapshot.SummaryDataPointSnapshot.builder()
            .labels(Labels.of(LABEL_HOST, poolName))
                .createdTimestampMillis()
            .value(driverFunction.applyAsLong(pool))
            .build())
    );
    return new GaugeSnapshot(metadata, data);
    SummaryMetricFamily metricFamily = new SummaryMetricFamily(metric, help, LABEL_NAMES);
    poolStatsMap.forEach((poolName, pool) -> metricFamily.addMetric(singletonList(poolName), driverFunction.applyAsLong(pool), 0));
    return metricFamily;
  }

}
