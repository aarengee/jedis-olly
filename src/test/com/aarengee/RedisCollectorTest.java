package com.aarengee;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Map;
import java.util.List;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Set;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RedisCollectorTest {
  private final Logger logger = LoggerFactory.getLogger(RedisCollectorTest.class);
  private static final String LOCALHOST = "127.0.0.1";

  @BeforeAll
  public static void setUp() {
    JedisCluster cluster = init();
    RedisCollector.getCollector().track(cluster);
  }

  private static JedisCluster init() {
    Set<HostAndPort> hostAndPortSet = new HashSet<>();
    for (int port = 30001; port < 30007; port++) {
      hostAndPortSet.add(new HostAndPort(LOCALHOST, port));
    }
    return new JedisCluster(hostAndPortSet, 1000);
  }

  @Test
  public void metricsAreEmitted() {
    Map<String, List<Collector.MetricFamilySamples.Sample>> metrics = new HashMap<>();
    for (Collector.MetricFamilySamples metricFamilySamples : Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples())) {
      metrics.put(metricFamilySamples.name, metricFamilySamples.samples);
    }
    verifyGaugeSampleValue("jedis_pool_num_active", metrics, 0.0);
    verifyGaugeSampleValue("jedis_pool_idle_connections", metrics, 0.0);
    verifyGaugeSampleValue("jedis_pool_num_waiters", metrics, 0.0);
    verifySummarySampleValue("jedis_pool_mean_borrow_wait_time_millis", metrics, 0.0);
    verifySummarySampleValue("jedis_pool_max_borrow_wait_time_millis", metrics, 0.0);
  }

  private void verifyGaugeSampleValue(String sampleName, Map<String, List<Collector.MetricFamilySamples.Sample>> metrics, double value) {
    assertNotNull(metrics.get(sampleName));
    for (Collector.MetricFamilySamples.Sample sample : metrics.get(sampleName)) {
      assertEquals(sample.name, sampleName);
      assertEquals(sample.value, value, 0);
      logger.info("Metrics verified for sample : {} with value : {}", sample.name, sample.value);
    }
  }

  private void verifySummarySampleValue(String sampleName, Map<String, List<Collector.MetricFamilySamples.Sample>> metrics, double value) {
    assertNotNull(metrics.get(sampleName));
    for (Collector.MetricFamilySamples.Sample sample : metrics.get(sampleName)) {
      assertTrue(sample.name.contains(sampleName));
      assertEquals(sample.value, value, 0);
      logger.info("Metrics verified for sample : {} with value : {}", sample.name, sample.value);
    }
  }
}
