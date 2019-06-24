package nb.kafka.operator.importer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.KafkaAdmin;
import nb.kafka.operator.Topic;
import nb.kafka.operator.TopicManager;
import nb.kafka.operator.util.PropertyUtil;
import nb.kafka.operator.watch.ConfigMapWatcher;

public class ConfigMapImporterTest {
  private KubernetesServer server;
  private AppConfig appConfig;

  @BeforeEach
  void setUp() {
    server = new KubernetesServer();
    server.before();

    appConfig = AppConfig.defaultConfig();
    appConfig.setBootstrapServers("localhost:9092");
  }

  @AfterEach
  void tearDown() {
    server.after();
    server = null;
  }

  @Test
  void testBuildConfigMapResource() {
    // Arrange
    String name = "test-topic";
    int partitions = 2;
    short replicationFactor = 1;

    String propsString = "compression.type=producer,retention.ms=3600000";
    Map<String, String> properties = PropertyUtil.stringToMap(propsString);
    Topic topic = new Topic(name, partitions, replicationFactor, properties, false);

    ConfigMapWatcher watcher = mock(ConfigMapWatcher.class);
    when(watcher.labels()).thenReturn(PropertyUtil.stringToMap("config=kafka-topic"));

    ConfigMapImporter importer = new ConfigMapImporter(null, watcher, null, appConfig);

    // Act
    ConfigMap cm = importer.buildConfigMapResource(topic);

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

  @Test
  void testImportWithNoTopics() throws InterruptedException, ExecutionException, TimeoutException {
    // Arrange
    AppConfig appConfig = new AppConfig();
    appConfig.setBootstrapServers("kafka:9092");
    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);

    KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);
    when(kafkaAdmin.listTopics()).thenReturn(Collections.emptySet());

    ConfigMapWatcher watcher = mock(ConfigMapWatcher.class);
    when(watcher.listTopicNames()).thenReturn(Collections.emptySet());

    TopicManager topicManager = new TopicManager(kafkaAdmin, appConfig);
    ConfigMapImporter configMapImporter = new ConfigMapImporter(null, watcher, topicManager, appConfig);

    // Act
    configMapImporter.importTopics();

    //Assert
    verify(kafkaAdmin).listTopics();
    verify(watcher).listTopicNames();
    verify(kafkaAdmin, never()).createTopic(any());
  }

  @Test
  void testImportTopics_NotEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    // Arrange
    String topicName = "test-topic";

    Node node = new Node(1, "kafka", 9092, "1");
    TopicDescription topicDescription = new TopicDescription(topicName, false,
        Arrays.asList(new TopicPartitionInfo(0, node, Arrays.asList(node), Arrays.asList(node))));

    KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);
    when(kafkaAdmin.listTopics()).thenReturn(new HashSet<>(Arrays.asList(topicName)));
    when(kafkaAdmin.describeTopic(any())).thenReturn(topicDescription);
    when(kafkaAdmin.describeConfigs(any())).thenReturn(new Config(Collections.emptyList()));

    ConfigMapWatcher watcher = mock(ConfigMapWatcher.class);
    when(watcher.listTopicNames()).thenReturn(new HashSet<>());

    KubernetesClient client = server.getClient();
    server.expect()
        .withPath("/api/v1/namespaces/test/configmaps")
        .andReturn(200, null)
        .once();

    TopicManager topicManager = new TopicManager(kafkaAdmin, appConfig);
    ConfigMapImporter configMapImporter = new ConfigMapImporter(client, watcher, topicManager, appConfig);

    // Act
    configMapImporter.importTopics();

    //Assert
    verify(kafkaAdmin).describeTopic(any());
  }

  @Test
  void testImportTopics_InvalidNames() throws InterruptedException, ExecutionException, TimeoutException {
    // Arrange
    String topicName = "__consumer-offsets";

    KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);
    when(kafkaAdmin.listTopics()).thenReturn(new HashSet<>(Arrays.asList(topicName)));

    ConfigMapWatcher watcher = mock(ConfigMapWatcher.class);
    when(watcher.listTopicNames()).thenReturn(new HashSet<>());

    TopicManager topicManager = new TopicManager(kafkaAdmin, appConfig);
    ConfigMapImporter configMapImporter = new ConfigMapImporter(null, watcher, topicManager, appConfig);

    // Act
    configMapImporter.importTopics();

    //Assert
    verify(kafkaAdmin, never()).describeTopic(any());
  }
}
