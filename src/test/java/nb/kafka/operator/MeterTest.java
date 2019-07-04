package nb.kafka.operator;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
    stopHttpEndpoint = Main.setupPrometheusRegistry(config.getMetricsPort());
    config.setBootstrapServers("localhost:9092");

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

    String metricEndpoint = "http://localhost:" + config.getMetricsPort() + "/metrics";
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
    MeterRegistry registry = meterMgr.getRegistry();

    // test that the topic exists in the list and have a proper gauge
    assertEquals(1, managedTopics.size());

    Gauge topicGauge = MeterUtils.filterMeterByTags(registry.get("managed.topic").gauges(),
            "operator-id", config.getOperatorId(),
            "topic-name", "test-topic").get();
    assertEquals(1D, topicGauge.value(), 0.1);

    // test deletion of an unexisting topic
    managedTopics.delete("unexisting-topic");
    assertEquals(1, managedTopics.size());

    // test deletion of an existing topic
    managedTopics.delete("test-topic");
    assertEquals(0, managedTopics.size());

    assertThrows(MeterNotFoundException.class, () -> MeterUtils.filterMeterByTags(
        registry.get("managed.topic").gauges(),
        "operator-id", config.getOperatorId(),
        "topic-name", "test-topic"));

    // test topic creation
    managedTopics.add(new Topic("test-topic2", 1, (short)1, null, false));
    assertEquals(1, managedTopics.size());

    topicGauge = MeterUtils.filterMeterByTags(registry.get("managed.topic").gauges(),
            "operator-id", config.getOperatorId(),
            "topic-name", "test-topic2").get();
    assertEquals(1D, topicGauge.value(), 0.1);

    // test create the same topic twice
    managedTopics.add(new Topic("test-topic2", 1, (short)1, null, false));
    assertEquals(1, managedTopics.size());

    topicGauge = MeterUtils.filterMeterByTags(registry.get("managed.topic").gauges(),
            "operator-id", config.getOperatorId(),
            "topic-name", "test-topic2").get();
    assertEquals(1D, topicGauge.value(), 0.1);
    assertEquals(1, registry.get("managed.topic").gauges().size());
  }

  @Test
  public void testOperatorStateGauge() throws Throwable {
    // Arrange
    KubernetesClient kubeClientMock = mock(KubernetesClient.class);
    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    ConfigMapWatcher watcherMock = mock(ConfigMapWatcher.class);

    // Act
    KafkaOperator operator = new KafkaOperator(config, kubeClientMock, kafkaAdminMock, watcherMock, meterMgr);
    MeterRegistry registry = meterMgr.getRegistry();

    Gauge stateGauge = MeterUtils.filterMeterByTags(registry.get("operator.state").gauges()).get();

    // Assert
    assertEquals(KafkaOperator.State.CREATED.ordinal(), stateGauge.value());

    operator.watch();
    assertEquals(KafkaOperator.State.RUNNING.ordinal(), stateGauge.value());

    operator.shutdown();
    assertEquals(KafkaOperator.State.STOPPED.ordinal(), stateGauge.value());
  }
}
