package nb.kafka.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

public class TopicManagerTest {

  @Test
  public void testCreateTopic() throws InterruptedException, ExecutionException, TopicCreationException {
    // Arrange
    String name = "test-topic";
    int partitions = 2;
    short replicationFactor = 1;
    Map<String, String> properties = null;
    Topic topic = new Topic(name, partitions, replicationFactor, properties, false);
    KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);
    when(kafkaAdmin.createTopic(any(NewTopic.class))).thenReturn(1);
    AppConfig config = new AppConfig();
    config.setDefaultReplicationFactor((short)1);
    TopicManager topicManager = new TopicManager(kafkaAdmin, config);

    // Act
    NewTopic newTopic = topicManager.createTopic(topic);

    // Assert
    verify(kafkaAdmin, only()).createTopic(any(NewTopic.class));
    assertEquals(name, newTopic.name());
    assertEquals(partitions, newTopic.numPartitions());
    assertEquals(replicationFactor, newTopic.replicationFactor());

    topicManager.close();
  }

  @Test
  public void testCreateTopicDefault() throws InterruptedException, ExecutionException, TopicCreationException {
    // Arrange
    String name = "test-topic";
    int numberOfBrokers = 3;
    short replicationFactor = 1;
    Topic topic = new Topic(name, -1, (short)-1, null, false);

    KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);
    when(kafkaAdmin.createTopic(any())).thenReturn(1);
    when(kafkaAdmin.numberOfBrokers()).thenReturn(numberOfBrokers);

    AppConfig config = new AppConfig();
    config.setDefaultReplicationFactor(replicationFactor);
    TopicManager topicManager = new TopicManager(kafkaAdmin, config);

    // Act
    NewTopic newTopic = topicManager.createTopic(topic);

    // Assert
    verify(kafkaAdmin).createTopic(any());
    verify(kafkaAdmin).numberOfBrokers();

    assertEquals(name, newTopic.name());
    assertEquals(numberOfBrokers, newTopic.numPartitions());
    assertEquals(replicationFactor, newTopic.replicationFactor());

    topicManager.close();
  }

  @Test
  public void testUpdateTopic() throws InterruptedException, ExecutionException {
    // Arrange
    String topicName = "test-topic";
    int partitions = 2;
    short replicationFactor = 1;

    Map<String, String> properties = null;
    Topic newTopic = new Topic(topicName, partitions, replicationFactor, properties, false);

    Node node = new Node(1, "kafka", 9092, "1");
    KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);
    TopicDescription topicDescription = new TopicDescription(topicName, false,
        Arrays.asList(new TopicPartitionInfo(partitions, node, Arrays.asList(node), Arrays.asList(node))));
    when(kafkaAdmin.describeTopic(newTopic.getName())).thenReturn(topicDescription);

    doNothing().when(kafkaAdmin)
        .createPartitions(newTopic.getName(), newTopic.getPartitions());
    doNothing().when(kafkaAdmin)
        .alterConfigs(newTopic);

    ConfigEntry configEntry = new ConfigEntry("compression.type", "producer");
    when(kafkaAdmin.describeConfigs(topicName)).thenReturn(new Config(Collections.singleton(configEntry)));

    AppConfig config = AppConfig.defaultConfig();
    TopicManager topicManager = new TopicManager(kafkaAdmin, config);

    // Act
    topicManager.updateTopic(newTopic);

    // Assert
    verify(kafkaAdmin).describeTopic(topicName);
    verify(kafkaAdmin).describeConfigs(topicName);
    verify(kafkaAdmin).alterConfigs(newTopic);

    topicManager.close();
  }

  @Test
  public void testDeleteTopic() throws InterruptedException, ExecutionException {
    // Arrange
    String topicName = "test-topic";
    KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);
    AppConfig config = new AppConfig();
    when(kafkaAdmin.deleteTopic(topicName)).thenReturn(1);
    TopicManager topicManager = new TopicManager(kafkaAdmin, config);

    // Act
    topicManager.deleteTopic(topicName);

    // Assert
    verify(kafkaAdmin, only()).deleteTopic(topicName);

    topicManager.close();
  }

  @Test
  public void testDescribeTopic() throws InterruptedException, ExecutionException {
    // Arrange
    String topicName = "test-topic";
    int partitions = 1;
    AppConfig config = new AppConfig();
    String expectedTopicDesc = "TopicDescriptor [name=test-topic, partitions=1, replicationFactor=1, acl=false, properties={compression.type=producer}]";
    KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);

    Node node = new Node(1, "kafka", 9092, "1");
    TopicDescription topicDescription = new TopicDescription(topicName, false,
        Arrays.asList(new TopicPartitionInfo(partitions, node, Arrays.asList(node), Arrays.asList(node))));
    when(kafkaAdmin.describeTopic(topicName)).thenReturn(topicDescription);

    ConfigEntry configEntry = new ConfigEntry("compression.type", "producer");
    when(kafkaAdmin.describeConfigs(topicName)).thenReturn(new Config(Collections.singleton(configEntry)));

    TopicManager topicManager = new TopicManager(kafkaAdmin, config);

    // Act
    PartitionedTopic partitionedTopic = topicManager.describeTopic(topicName);

    // Assert
    verify(kafkaAdmin).describeTopic(topicName);
    verify(kafkaAdmin).describeConfigs(topicName);
    assertEquals(topicName, partitionedTopic.getName());
    assertEquals(expectedTopicDesc, partitionedTopic.toString());
    assertEquals(partitions, partitionedTopic.getPartitions());

    topicManager.close();
  }

  @Test
  public void testListTopics() throws InterruptedException, ExecutionException, TimeoutException {
    // Arrange
    KafkaAdmin kafkaAdmin = mock(KafkaAdmin.class);
    Set<String> expectedTopics = new HashSet<String>();
    expectedTopics.add("test-topic-1");
    expectedTopics.add("test-topic-2");
    when(kafkaAdmin.listTopics()).thenReturn(expectedTopics);
    AppConfig config = new AppConfig();
    TopicManager topicManager = new TopicManager(kafkaAdmin, config);

    // Act
    Set<String> topics = topicManager.listTopics();

    // Assert
    assertNotNull(topics);
    assertEquals(expectedTopics, topics);

    topicManager.close();
  }

}
