package nb.kafka.operator.watch;

import static nb.common.App.metrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;

public class ConfigMapWatcherTest {
  @BeforeEach
  void setUp() {
  }

  @AfterEach
  void tearDown() {
    metrics().remove("managed-topics");
  }

  @Test
  void testBuildTopicModel() {
    // Arrange
    String topicName = "test-topic";
    int partitions = 20;
    int replicationFactor = 2;
    String properties = "retention.ms=2000000";

    Map<String, String> data = new HashMap<>();
    data.put("partitions", Integer.toString(partitions));
    data.put("properties", properties);
    data.put("replication-factor", Integer.toString(replicationFactor));

    ObjectMeta metadata = new ObjectMeta();
    metadata.setName(topicName);
    Map<String, String> labels = Collections.singletonMap("config", "kafka-topic");
    metadata.setLabels(labels);
    ConfigMap cm = new ConfigMap("v1", data, "configMap", metadata);

    AppConfig appConfig = new AppConfig();
    appConfig.setKafkaUrl("kafka:9092");
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);
    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(null, appConfig)) {
      // Act
      Topic topic = configMapWatcher.buildTopicModel(cm);

      // Assert
      assertNotNull(topic);
      assertEquals(topicName, topic.getName());
      assertEquals(partitions, topic.getPartitions());
      assertEquals(replicationFactor, topic.getReplicationFactor());
      assertEquals("retention.ms", topic.getProperties()
          .entrySet()
          .iterator()
          .next()
          .getKey());
      assertEquals("2000000", topic.getProperties()
          .entrySet()
          .iterator()
          .next()
          .getValue());
    }
  }

  @Test
  void testLabels() {
    // Arrange
    AppConfig appConfig = new AppConfig();
    appConfig.setKafkaUrl("kafka:9092");
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);
    try (ConfigMapWatcher configMapManager = new ConfigMapWatcher(null, appConfig)) {
      // Act
      Map<String, String> labels = configMapManager.labels();

      // Assert
      assertNotNull(labels);
      assertEquals(1, labels.size());
      assertEquals(standardLabels, labels);
    }
  }
}