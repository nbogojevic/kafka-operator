package nb.kafka.operator.watch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapListBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.WatchEventBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;
import nb.kafka.operator.util.Holder;


public class ConfigMapWatcherTest {
  private final static String WATCH_PATH = "/api/v1/namespaces/test/configmaps?labelSelector=config%3Dkafka-topic&watch=true";

  private KubernetesServer server;
  private AppConfig appConfig;

  @BeforeEach
  void setUp() {
    server = new KubernetesServer();
    server.before();
    
    appConfig = AppConfig.defaultConfig();
    appConfig.setKafkaUrl("localhost:9092");

    Map<String, String> standardLabels = Collections.singletonMap("config", "kafka-topic");
    appConfig.setStandardLabels(standardLabels);
  }

  @AfterEach
  void tearDown() {
    server.after();
    server = null;
  }

  @Test
  void testBuildTopicModel() {
    // Arrange
    String topicName = "test-topic";
    int partitions = 20;
    short replicationFactor = 2;
    long retentionTime = 2000000L;

    ConfigMap cm = makeConfigMap(topicName, partitions, replicationFactor, retentionTime) ;
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
  void testWatchConfigMapCreateTopic() throws InterruptedException {
    String topicName = "test-topic";
    int partitions = 20;
    short replicationFactor = 3;
    long retentionTime = 3600000L;

    ConfigMap cm = makeConfigMap(topicName, partitions, replicationFactor, retentionTime);

    KubernetesClient client = server.getClient();
    CountDownLatch crudLatch = new CountDownLatch(1);

    server.expect()
        .withPath(WATCH_PATH)
        .andUpgradeToWebSocket()
        .open()
        .waitFor(500)
        .andEmit(new WatchEventBuilder().withConfigMapObject(cm).withType("ADDED").build())
        .done()
        .once();

    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      Holder<Topic> holder = new Holder<>();
      configMapWatcher.setOnCreateListener((topic) -> {
        holder.set(topic);
        crudLatch.countDown();
      });
      configMapWatcher.watch();
      assertTrue(crudLatch.await(10, TimeUnit.SECONDS));

      Topic toCreateTopic = holder.get();
      assertNotNull(toCreateTopic);
      assertEquals(topicName, toCreateTopic.getName());
      assertEquals(partitions, toCreateTopic.getPartitions());
      assertEquals(replicationFactor, toCreateTopic.getReplicationFactor());
    }
  }

  @Test
  void testWatchConfigMapUpdateTopic() throws InterruptedException {
    String topicName = "test-topic";
    int partitions = 20;
    short replicationFactor = 3;
    long retentionTime = 3600000L;

    ConfigMap cm = makeConfigMap(topicName, partitions, replicationFactor, retentionTime);

    KubernetesClient client = server.getClient();
    CountDownLatch crudLatch = new CountDownLatch(1);

    server.expect()
      .withPath(WATCH_PATH)
      .andUpgradeToWebSocket()
      .open()
      .waitFor(500)
      .andEmit(new WatchEventBuilder().withConfigMapObject(cm).withType("MODIFIED").build())
      .done()
      .once();

    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      Holder<Topic> holder = new Holder<>();
      configMapWatcher.setOnUpdateListener((topic) -> {
        holder.set(topic);
        crudLatch.countDown();
      });
      configMapWatcher.watch();
      assertTrue(crudLatch.await(10, TimeUnit.SECONDS));
      
      Topic toUpdateTopic = holder.get();
      assertNotNull(toUpdateTopic);
      assertEquals(topicName, toUpdateTopic.getName());
      assertEquals(partitions, toUpdateTopic.getPartitions());
      assertEquals(replicationFactor, toUpdateTopic.getReplicationFactor());
    }
  }

  @Test
  void testWatchConfigDeleteTopic() throws InterruptedException {
    String topicName = "test-topic";
    int partitions = 20;
    short replicationFactor = 3;
    long retentionTime = 3600000L;

    ConfigMap cm = makeConfigMap(topicName, partitions, replicationFactor, retentionTime);

    KubernetesClient client = server.getClient();
    CountDownLatch crudLatch = new CountDownLatch(1);

    server.expect()
        .withPath(WATCH_PATH)
        .andUpgradeToWebSocket()
        .open()
        .waitFor(500)
        .andEmit(new WatchEventBuilder().withConfigMapObject(cm).withType("DELETED").build())
        .done()
        .once();

    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      Holder<String> holder = new Holder<>();
      configMapWatcher.setOnDeleteListener((topic) -> {
        holder.set(topic);
        crudLatch.countDown();
      });
      configMapWatcher.watch();
      assertTrue(crudLatch.await(10, TimeUnit.SECONDS));
      
      String toDeleteTopic = holder.get();
      assertNotNull(toDeleteTopic);
      assertEquals(topicName, toDeleteTopic);
    }
  }

  @Test
  void testWatchConfigMapErrorTopic() throws InterruptedException, KubernetesClientException {
    String topicName = "test-topic";
    int partitions = 20;
    short replicationFactor = 3;
    long retentionTime = 3600000L;

    ConfigMap cm = makeConfigMap(topicName, partitions, replicationFactor, retentionTime);

    KubernetesClient client = server.getClient();
    CountDownLatch crudLatch = new CountDownLatch(1);

    server.expect()
      .withPath(WATCH_PATH)
      .andUpgradeToWebSocket()
      .open()
      .waitFor(500)
      .andEmit(new WatchEventBuilder().withConfigMapObject(cm).withType("ERROR").build())
      .done()
      .once();

    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      configMapWatcher.watch();
      assertFalse(crudLatch.await(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void testListTopics() {
    String topicName = "test-topic";
    int partitions = 20;
    short replicationFactor = 3;
    long retentionTime = 3600000L;

    ConfigMap cm = makeConfigMap(topicName, partitions, replicationFactor, retentionTime);

    KubernetesClient client = server.getClient();

    server.expect()
        .withPath("/api/v1/namespaces/test/configmaps?labelSelector=config%3Dkafka-topic")
        .andReturn(200, new ConfigMapListBuilder().addToItems(cm).build())
        .once();

    try (ConfigMapWatcher configMapWatcher = new ConfigMapWatcher(client, appConfig)) {
      // Act
      List<Topic> topics = configMapWatcher.listTopics();
      assertNotNull(topics);
      assertEquals(1, topics.size());

      Topic topic = topics.get(0);
      assertNotNull(topic);
      assertEquals(topicName, topic.getName());
      assertEquals(partitions, topic.getPartitions());
      assertEquals(replicationFactor, topic.getReplicationFactor());
    }
  }
  
  private ConfigMap makeConfigMap(String topicName, int partitions, short replicationFactor, long retentionTime) {
    Map<String, String> data = new HashMap<>();
    data.put("partitions", Integer.toString(partitions));
    data.put("properties", "retention.ms=" + retentionTime);
    data.put("replication-factor", Integer.toString(replicationFactor));
    
    ObjectMeta metadata = new ObjectMeta();
    metadata.setName(topicName);

    Map<String, String> labels = Collections.singletonMap("config", "kafka-topic");
    metadata.setLabels(labels);
    ConfigMap cm = new ConfigMap("v1", data, "ConfigMap", metadata);
    cm.setData(data);
    return cm;
  }
}


