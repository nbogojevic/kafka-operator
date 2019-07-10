package nb.kafka.operator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import nb.kafka.operator.util.WhiteboxUtil;
import nb.kafka.operator.watch.AbstractTopicWatcher;

public class KafkaOperatorTest {
  private KafkaOperator operator;
  private AppConfig config = AppConfig.defaultConfig();
  private KubernetesServer kubernetesServerMock;
  private KafkaAdmin kafkaAdminMock;

  @BeforeEach
  void setUp() {
    // kafka admin base mock
    kafkaAdminMock = mock(KafkaAdmin.class);

    // kubernetes server mock
    kubernetesServerMock = new KubernetesServer();
    kubernetesServerMock.before();

    kubernetesServerMock.expect()
      .withPath("/api/v1/namespaces/test/configmaps?labelSelector=config%3Dkafka-topic")
      .andReturn(200, new ConfigMapList())
      .once();

    operator = new KafkaOperator(config, kubernetesServerMock.getClient(), kafkaAdminMock);
  }

  @AfterEach
  void tearDown() {
    operator = null;
    kubernetesServerMock.after();
  }

  @Test
  public void testCreateTopicNullArg() throws Throwable {
    // Arrange
    Topic topic = null;

    // Act
    emitCreate(operator, topic);

    // Assert
    verify(kafkaAdminMock, never()).createTopic(any(NewTopic.class));
  }

  @Test
  public void testCreateTopic() throws Throwable {
    // Arrange
    Topic topic = new Topic("test-topic", 1, (short)1, Collections.singletonMap("retention.ms", "3600000"), false);

    when(kafkaAdminMock.createTopic(any(NewTopic.class))).thenReturn(1);

    // Act
    emitCreate(operator, topic);

    // Assert
    verify(kafkaAdminMock, atLeast(1)).createTopic(any(NewTopic.class));
    verify(kafkaAdminMock, atMost(1)).createTopic(any(NewTopic.class));
  }

  @Test
  public void testCreateTopicAlreadyExistsInKafka() throws Throwable {
    // Arrange
    Topic topic = new Topic("test-topic", 1, (short)1, Collections.singletonMap("retention.ms", "3600000"), false);
    TopicDescription existingTopic = buildTopicDescription(topic);
    Config existingTopicProperties = buildTopicConfig(topic);

    when(kafkaAdminMock.createTopic(any(NewTopic.class))).thenThrow(new ExecutionException(new TopicExistsException("exists")));
    when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
    when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);

    // Act
    emitCreate(operator, topic);

    // Assert
    verify(kafkaAdminMock, atLeast(1)).createTopic(any(NewTopic.class));
    verify(kafkaAdminMock, atMost(1)).createTopic(any(NewTopic.class));

    // no update should never been called because new and existing topic are identical
    verify(kafkaAdminMock, atLeast(0)).alterConfigs(topic);
    verify(kafkaAdminMock, atMost(0)).alterConfigs(topic);
    verify(kafkaAdminMock, atLeast(0)).createPartitions(any(String.class), any(Integer.class));
    verify(kafkaAdminMock, atMost(0)).createPartitions(any(String.class), any(Integer.class));
  }

 @Test
  public void testUpdateTopicProperties() throws Throwable {
    // Arrange
    Topic updatedTopic = new Topic("test-topic", 1, (short)1, Collections.singletonMap("retention.ms", "3600000"),
        false);
    TopicDescription existingTopic = buildTopicDescription(updatedTopic);
    Config existingTopicProperties = new Config(Collections.singletonList(new ConfigEntry("retention.ms", "200000")));

    when(kafkaAdminMock.listTopics()).thenReturn(Collections.singleton(updatedTopic.getName()));
    when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
    when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);

    // Act
    emitUpdate(operator, updatedTopic);

    // Assert
    verify(kafkaAdminMock, atLeast(1)).alterConfigs(updatedTopic);
    verify(kafkaAdminMock, atMost(1)).alterConfigs(updatedTopic);
    verify(kafkaAdminMock, atLeast(0)).createPartitions(any(String.class), any(Integer.class));
    verify(kafkaAdminMock, atMost(0)).createPartitions(any(String.class), any(Integer.class));
    verify(kafkaAdminMock, atLeast(0)).createTopic(any(NewTopic.class));
    verify(kafkaAdminMock, atMost(0)).createTopic(any(NewTopic.class));
  }

 @Test
 public void testUpdateTopicPartitions() throws Throwable {
   // Arrange
   Topic updatedTopic = new Topic("test-topic", 3, (short)1, Collections.singletonMap("retention.ms", "3600000"),
       false);
   TopicDescription existingTopic = buildTopicDescription(updatedTopic.getName(), 1, updatedTopic.getReplicationFactor());
   Config existingTopicProperties = buildTopicConfig(updatedTopic);

   when(kafkaAdminMock.listTopics()).thenReturn(Collections.singleton(updatedTopic.getName()));
   when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
   when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);

   // Act
   emitUpdate(operator, updatedTopic);

   // Assert
   verify(kafkaAdminMock, atLeast(0)).alterConfigs(updatedTopic);
   verify(kafkaAdminMock, atMost(0)).alterConfigs(updatedTopic);
   verify(kafkaAdminMock, atLeast(1)).createPartitions(updatedTopic.getName(), updatedTopic.getPartitions());
   verify(kafkaAdminMock, atMost(1)).createPartitions(updatedTopic.getName(), updatedTopic.getPartitions());
   verify(kafkaAdminMock, atLeast(0)).createTopic(any(NewTopic.class));
   verify(kafkaAdminMock, atMost(0)).createTopic(any(NewTopic.class));
 }

 @Test
 public void testUpdateTopicPartitionsAndProperties() throws Throwable {
   // Arrange
   Topic updatedTopic = new Topic("test-topic", 3, (short)1, Collections.singletonMap("retention.ms", "3600000"),
       false);
   TopicDescription existingTopic = buildTopicDescription(updatedTopic.getName(), 1, updatedTopic.getReplicationFactor());
   Config existingTopicProperties = new Config(Collections.singletonList(new ConfigEntry("retention.ms", "200000")));

   when(kafkaAdminMock.listTopics()).thenReturn(Collections.singleton(updatedTopic.getName()));
   when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
   when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);

   // Act
   emitUpdate(operator, updatedTopic);

   // Assert
   verify(kafkaAdminMock, atLeast(1)).alterConfigs(updatedTopic);
   verify(kafkaAdminMock, atMost(1)).alterConfigs(updatedTopic);
   verify(kafkaAdminMock, atLeast(1)).createPartitions(updatedTopic.getName(), updatedTopic.getPartitions());
   verify(kafkaAdminMock, atMost(1)).createPartitions(updatedTopic.getName(), updatedTopic.getPartitions());
   verify(kafkaAdminMock, atLeast(0)).createTopic(any(NewTopic.class));
   verify(kafkaAdminMock, atMost(0)).createTopic(any(NewTopic.class));
 }

  @Test
  public void testDeleteTopic() throws Throwable {
    // Arrange
    config.setEnableTopicDelete(true);

    Topic deletedTopic = new Topic("test-topic", 1, (short)1, Collections.singletonMap("retention.ms", "3600000"),
        false);

    // Act
    emitDelete(operator, deletedTopic.getName());

    // Assert
    verify(kafkaAdminMock, atLeast(1)).deleteTopic(deletedTopic.getName());
    verify(kafkaAdminMock, atMost(1)).deleteTopic(deletedTopic.getName());
  }

  @Test
  public void testDeleteTopicUnallowed() throws Throwable {
    // Arrange
    config.setEnableTopicDelete(false);

    Topic deletedTopic = new Topic("test-topic", 1, (short)1, Collections.singletonMap("retention.ms", "3600000"),
        false);

    // Act
    emitDelete(operator, deletedTopic.getName());

    // Assert
    verify(kafkaAdminMock, atLeast(0)).deleteTopic(deletedTopic.getName());
    verify(kafkaAdminMock, atMost(0)).deleteTopic(deletedTopic.getName());
  }

  private void emitCreate(KafkaOperator operator, Topic topic) throws Throwable {
    AbstractTopicWatcher watcher = WhiteboxUtil.readField(operator, "topicWatcher");
    WhiteboxUtil.runMethod(AbstractTopicWatcher.class, "emitCreate", Topic.class, watcher, topic);
  }

  private void emitUpdate(KafkaOperator operator, Topic topic) throws Throwable {
    AbstractTopicWatcher watcher = WhiteboxUtil.readField(operator, "topicWatcher");
    WhiteboxUtil.runMethod(AbstractTopicWatcher.class, "emitUpdate", Topic.class, watcher, topic);
  }

  private void emitDelete(KafkaOperator operator, String topic) throws Throwable {
    AbstractTopicWatcher watcher = WhiteboxUtil.readField(operator, "topicWatcher");
    WhiteboxUtil.runMethod(AbstractTopicWatcher.class, "emitDelete", String.class, watcher, topic);
  }

  private TopicDescription buildTopicDescription(Topic topic) {
    return buildTopicDescription(topic.getName(), topic.getPartitions(), topic.getReplicationFactor());
  }

  private TopicDescription buildTopicDescription(String name, int partitions, int replicationFactor) {
    Node node = new Node(0, "localhost", 9092);
    List<TopicPartitionInfo> topicPartitions = IntStream.range(0, partitions)
        .mapToObj((partition) -> {
          List<Node> replicas = IntStream.range(0, replicationFactor)
              .mapToObj(replica -> node)
              .collect(Collectors.toList());
          return new TopicPartitionInfo(partition, node, replicas, new ArrayList<>(replicas));
        })
        .collect(Collectors.toList());
    return new TopicDescription(name, false, topicPartitions);
  }

  private Config buildTopicConfig(Topic topic) {
    return buildTopicConfig(topic.getProperties());
  }

  private Config buildTopicConfig(Map<String, String> properties) {
    List<ConfigEntry> entries = properties.entrySet()
        .stream()
        .map(e -> new ConfigEntry(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
    return new Config(entries);
  }
}
