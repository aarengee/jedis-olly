package com.aarengee;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class RedisCollectorTest {

  private final Logger logger = LoggerFactory.getLogger(RedisCollectorTest.class);
  private ApplicationConfiguration applicationConfiguration;
  private JedisCluster jedisCluster;
  Set<HostAndPort> hostAndPortSet = new HashSet<>();

  @Before
  public void setUp() throws Exception {
    applicationConfiguration = Figaro.configure(null);
    jedisCluster = createCluster(applicationConfiguration);
    RedisCollector.getCollector().track(jedisCluster);
  }

  private JedisCluster createCluster(ApplicationConfiguration applicationConfiguration) {
    try {
      String[] nodes = applicationConfiguration.getValueAsString(AppConfig.REDIS_CLUSTER_HOSTNAMES).split(",");
      for (String node : nodes) {
        hostAndPortSet.add(new HostAndPort(node.trim().split(":")[0], Integer.parseInt(node.trim().split(":")[1])));
      }
      return new JedisCluster(hostAndPortSet, applicationConfiguration.getValueAsInt(AppConfig.REDIS_CLUSTER_TIMEOUT));
    } catch (Exception e) {
      throw e;
    }
  }

  @Test
  public void metricsAreEmitted() {
    Map<String, List<Collector.MetricFamilySamples.Sample>> metrics = new HashMap<>();
    for (Collector.MetricFamilySamples metricFamilySamples: Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples())) {
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
    for(Collector.MetricFamilySamples.Sample sample : metrics.get(sampleName)) {
      assertEquals(sample.name, sampleName);
      assertEquals(sample.value, value, 0);
      logger.info("Metrics verified for sample : {} with value : {}", sample.name, sample.value);
    }
  }

  private void verifySummarySampleValue(String sampleName, Map<String, List<Collector.MetricFamilySamples.Sample>> metrics, double value) {
    assertNotNull(metrics.get(sampleName));
    for(Collector.MetricFamilySamples.Sample sample : metrics.get(sampleName)) {
      assertTrue(sample.name.contains(sampleName));
      assertEquals(sample.value, value, 0);
      logger.info("Metrics verified for sample : {} with value : {}", sample.name, sample.value);
    }
  }
}