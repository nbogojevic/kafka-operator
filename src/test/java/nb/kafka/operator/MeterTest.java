package nb.kafka.operator;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Scanner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import nb.common.App;
import nb.kafka.operator.util.MeterManager;
import nb.kafka.operator.util.MeterUtils;
import nb.kafka.operator.watch.ConfigMapWatcher;

public class MeterTest {
  private Runnable stopHttpEndpoint;
  private AppConfig config = AppConfig.defaultConfig();
  private MeterManager meterMgr;

  @BeforeEach
  void setUp() {
    // make the test runnable without an Internet connection
    stopHttpEndpoint = App.setupPrometheusRegistry(config.getPrometheusEndpointPort());
    config.setKafkaUrl("localhost:9092");

    meterMgr = new MeterManager(new SimpleMeterRegistry());
  }

  @AfterEach
  void tearDown() {
    stopHttpEndpoint.run();
    meterMgr.close();
  }

  @Test
  public void testPrometheusEndpoint() throws Throwable {
    // Arrange
    KubernetesClient kubeClientMock = mock(KubernetesClient.class);
    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    ConfigMapWatcher watcherMock = mock(ConfigMapWatcher.class);
    when(watcherMock.listTopics()).thenReturn(Arrays.asList(new Topic("test-topic", 1, (short)1, null, false)));

    // Act
    new KafkaOperator(config, kubeClientMock, kafkaAdminMock, watcherMock, MeterManager.defaultMeterManager());

    String metricEndpoint = "http://localhost:" + config.getPrometheusEndpointPort() + "/metrics";
    URLConnection connection = new URL(metricEndpoint).openConnection();

    connection.setDoOutput(true);
    connection.connect();

    // Assert
    try (Scanner scanner = new Scanner(connection.getInputStream(), "UTF-8")) {
      scanner.useDelimiter("\\A");

      assertTrue(scanner.hasNext());

      String result = scanner.next();
      assertNotNull(result);
      assertNotEquals(0, result.length());
    }
  }

  @Test
  public void testTopicMetrics() throws Throwable {
    // Arrange
    KubernetesClient kubeClientMock = mock(KubernetesClient.class);
    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    ConfigMapWatcher watcherMock = mock(ConfigMapWatcher.class);
    when(watcherMock.listTopics()).thenReturn(Arrays.asList(new Topic("test-topic", 1, (short)1, null, false)));

    // Act
    new KafkaOperator(config, kubeClientMock, kafkaAdminMock, watcherMock, meterMgr);
    MeterRegistry registry = meterMgr.getRegistry();

    Gauge topicCount = MeterUtils.filterMeterByTags(registry.get("managed.topics").gauges(), "operator-id", config.getOperatorId()).get();
    Gauge topicLiveness = MeterUtils.filterMeterByTags(registry.get("managed.topic").gauges(),
        "operator-id", config.getOperatorId(),
        "topic-name", "test-topic").get();

    // Assert
    assertEquals(1, topicCount.value());
    assertEquals(1, topicLiveness.value());
  }

  @Test
  public void testManagedTopicList() throws Throwable {
    ManagedTopicList managedTopics = new ManagedTopicList(meterMgr, config,
        Arrays.asList(new Topic("test-topic", 1, (short)1, null, false)));

    assertEquals(1, managedTopics.size());

    managedTopics.delete("unexisting-topic");
    assertEquals(1, managedTopics.size());

    managedTopics.delete("test-topic");
    assertEquals(0, managedTopics.size());

    managedTopics.add(new Topic("test-topic2", 1, (short)1, null, false));
    assertEquals(1, managedTopics.size());

    managedTopics.add(new Topic("test-topic2", 1, (short)1, null, false));
    assertEquals(1, managedTopics.size());
  }
}
