package nb.kafka.operator.importer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ConfigMap;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;
import nb.kafka.operator.util.PropertyUtil;
import nb.kafka.operator.watch.ConfigMapWatcher;

public class ConfigMapImporterTest {
  @Test
  void testBuildConfigMapResource() {
    // Arrange
    String name = "test-topic";
    int partitions = 2;
    short replicationFactor = 1;

    String propsString = "compression.type=producer,retention.ms=3600000";
    Map<String, String> properties = PropertyUtil.stringToMap(propsString);
    Topic topic = new Topic(name, partitions, replicationFactor, properties, false);

    AppConfig appConfig = new AppConfig();
    appConfig.setKafkaUrl("kafka:9092");
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);

    ConfigMapWatcher watcher = mock(ConfigMapWatcher.class);
    when(watcher.labels()).thenReturn(PropertyUtil.stringToMap("config=kafka-topic"));

    ConfigMapImporter configMapManager = new ConfigMapImporter(null, watcher, null, appConfig);
    // Act
    ConfigMap cm = configMapManager.buildConfigMapResource(topic);

    // Assert
    assertEquals("2", cm.getData().get("partitions"));
    assertEquals("test-topic", cm.getData().get("name"));
    assertEquals("1", cm.getData().get("replication-factor"));
    assertEquals("false", cm.getData().get("acl"));

    for (Entry<String, String> entry : properties.entrySet()) {
      String kv = entry.getKey() + "=" + entry.getValue();
      assertTrue(cm.getData().get("properties").contains(kv));
    }

    assertEquals("kafka-topic", cm.getMetadata().getLabels().get("config"));
  }
}
