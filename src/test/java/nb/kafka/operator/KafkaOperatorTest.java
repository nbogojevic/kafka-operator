package nb.kafka.operator;

import static nb.common.App.metrics;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

import nb.kafka.operator.util.WhiteboxUtil;
import nb.kafka.operator.watch.AbstractTopicWatcher;

public class KafkaOperatorTest {
  private KafkaOperator operator;
  private AppConfig config = KafkaOperator.loadConfig();

  @BeforeEach
  void setUp() {
    config.setKafkaUrl("localhost:9092");
    operator = new KafkaOperator(config);
  }

  @AfterEach
  void tearDown() {
    operator = null;
    metrics().remove("managed-topics");
  }

  @Test
  public void testCreateTopic() throws Throwable {
    // Arrange
    Topic topic = new Topic("test-topic", 1, (short)1, Collections.singletonMap("retention.ms", "3600000"), false);

    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    when(kafkaAdminMock.createTopic(any(NewTopic.class))).thenReturn(1);

    injectKafkaAdminMock(operator, kafkaAdminMock);

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
    
    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    when(kafkaAdminMock.createTopic(any(NewTopic.class))).thenThrow(new ExecutionException(new TopicExistsException("exists")));
    when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
    when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);
    
    injectKafkaAdminMock(operator, kafkaAdminMock);

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
    
    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    when(kafkaAdminMock.listTopics()).thenReturn(Collections.singleton(updatedTopic.getName()));
    when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
    when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);

    injectKafkaAdminMock(operator, kafkaAdminMock);

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

   KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
   when(kafkaAdminMock.listTopics()).thenReturn(Collections.singleton(updatedTopic.getName()));
   when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
   when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);

   injectKafkaAdminMock(operator, kafkaAdminMock);

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

   KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
   when(kafkaAdminMock.listTopics()).thenReturn(Collections.singleton(updatedTopic.getName()));
   when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
   when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);

   injectKafkaAdminMock(operator, kafkaAdminMock);

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
  public void testUpdateTopicFailReplicationFactorChange() throws Throwable {
 // Arrange
    Topic updatedTopic = new Topic("test-topic", 1, (short)3, Collections.singletonMap("retention.ms", "3600000"),
        false);
    TopicDescription existingTopic = buildTopicDescription(updatedTopic.getName(), updatedTopic.getPartitions(), 1);
    Config existingTopicProperties = buildTopicConfig(updatedTopic);

    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    when(kafkaAdminMock.listTopics()).thenReturn(Collections.singleton(updatedTopic.getName()));
    when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
    when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);

    injectKafkaAdminMock(operator, kafkaAdminMock);

    // Act
    assertThrows(IllegalArgumentException.class, () -> emitUpdate(operator, updatedTopic));

    // Assert
    verify(kafkaAdminMock, atLeast(0)).alterConfigs(updatedTopic);
    verify(kafkaAdminMock, atMost(0)).alterConfigs(updatedTopic);
    verify(kafkaAdminMock, atLeast(0)).createPartitions(updatedTopic.getName(), updatedTopic.getPartitions());
    verify(kafkaAdminMock, atMost(0)).createPartitions(updatedTopic.getName(), updatedTopic.getPartitions());
    verify(kafkaAdminMock, atLeast(0)).createTopic(any(NewTopic.class));
    verify(kafkaAdminMock, atMost(0)).createTopic(any(NewTopic.class));
  }

  @Test
  public void testUpdateTopicFailPartitionsReduction() throws Throwable {
    // Arrange
    Topic updatedTopic = new Topic("test-topic", 1, (short)1, Collections.singletonMap("retention.ms", "3600000"),
        false);
    TopicDescription existingTopic = buildTopicDescription(updatedTopic.getName(), 3, updatedTopic.getReplicationFactor());
    Config existingTopicProperties = buildTopicConfig(updatedTopic);

    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);
    when(kafkaAdminMock.listTopics()).thenReturn(Collections.singleton(updatedTopic.getName()));
    when(kafkaAdminMock.describeTopic(any(String.class))).thenReturn(existingTopic);
    when(kafkaAdminMock.describeConfigs(any(String.class))).thenReturn(existingTopicProperties);

    injectKafkaAdminMock(operator, kafkaAdminMock);

    // Act
    assertThrows(IllegalArgumentException.class, () -> emitUpdate(operator, updatedTopic));

    // Assert
    verify(kafkaAdminMock, atLeast(0)).alterConfigs(updatedTopic);
    verify(kafkaAdminMock, atMost(0)).alterConfigs(updatedTopic);
    verify(kafkaAdminMock, atLeast(0)).createPartitions(updatedTopic.getName(), updatedTopic.getPartitions());
    verify(kafkaAdminMock, atMost(0)).createPartitions(updatedTopic.getName(), updatedTopic.getPartitions());
    verify(kafkaAdminMock, atLeast(0)).createTopic(any(NewTopic.class));
    verify(kafkaAdminMock, atMost(0)).createTopic(any(NewTopic.class));
  }
  
  @Test
  public void testDeleteTopic() throws Throwable {
    // Arrange
    Topic deletedTopic = new Topic("test-topic", 1, (short)1, Collections.singletonMap("retention.ms", "3600000"),
        false);
    KafkaAdmin kafkaAdminMock = mock(KafkaAdmin.class);

    injectKafkaAdminMock(operator, kafkaAdminMock);

    // Act
    emitDelete(operator, deletedTopic.getName());

    // Assert
    verify(kafkaAdminMock, atLeast(1)).deleteTopic(deletedTopic.getName());
    verify(kafkaAdminMock, atMost(1)).deleteTopic(deletedTopic.getName());
  }
  
  private void injectKafkaAdminMock(KafkaOperator operator, KafkaAdmin kafkaAdmin) throws Exception {
    TopicManager topicMgr = new TopicManager(kafkaAdmin, config);
    WhiteboxUtil.injectField(operator, "topicManager", topicMgr);
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
