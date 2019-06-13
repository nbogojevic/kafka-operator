package nb.kafka.operator;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import nb.kafka.operator.util.PropertyUtil;
import org.apache.kafka.clients.admin.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class KafkaAdminImplTest {

  @RegisterExtension
  public static final SharedKafkaTestResource kafkaBroker = new SharedKafkaTestResource ().withBrokers(1);
  private KafkaAdminImpl kafkaAdmin;

  @BeforeEach
  void setUp() {
    kafkaAdmin = new KafkaAdminImpl(kafkaBroker.getKafkaConnectString(), null);
  }

  @AfterEach
  void tearDown() {
    kafkaAdmin = null;
  }

  @Test
  void deleteTopicTest() throws Exception{

    String topicName = "topic-to-delete";
    int partions = 20;
    short replicationFactor = 1;

    NewTopic topic = new NewTopic(topicName, partions, replicationFactor);
    KafkaTestUtils kafkaUtils = kafkaBroker.getKafkaTestUtils();
    kafkaUtils.createTopic(topicName,partions,replicationFactor);
    int topicDeleted = kafkaAdmin.deleteTopic(topicName);
    assertEquals(1, topicDeleted);
    assertThrows(RuntimeException.class, () ->  kafkaUtils.describeTopic(topicName));
  }

  @Test
  void createTopicTest() throws Exception {

    String topicName = "topic-to-be-created";
    int partitions = 20;
    short replicationFactor = 1;
    String properties = "retention.ms=2000000";

    NewTopic topic = new NewTopic(topicName,partitions,replicationFactor);
    topic.configs(PropertyUtil.propertiesFromString(properties));
    int topicCreated = kafkaAdmin.createTopic(topic);
    assertEquals(1, topicCreated);
    KafkaTestUtils kafkaUtils = kafkaBroker.getKafkaTestUtils();
    TopicDescription topicDescription =  kafkaUtils.describeTopic(topicName);
    assertEquals(topicName, topicDescription.name());
    assertEquals(partitions, topicDescription.partitions().size());
    Config config = kafkaAdmin.describeConfigs(topicName);
    Map<String, String> props = config.entries()
      .stream()
      .filter(x -> !x.isDefault() && !x.isReadOnly())
      .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
    Map.Entry<String, String> configEntry =  props.entrySet().iterator().next();
    assertEquals("retention.ms", configEntry.getKey());
    assertEquals("2000000", configEntry.getValue());

  }

  @Test
  void numberOfBrokersTest() throws Exception {

    int brokers = kafkaAdmin.numberOfBrokers();
    assertEquals(1, brokers);

  }

  @Test
  void listTopics() throws Exception{

    String topicName = "topic-to-be-listed";
    int partitions = 20;
    short replicationFactor = 1;
    NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
    KafkaTestUtils kafkaUtils = kafkaBroker.getKafkaTestUtils();
    kafkaUtils.createTopic(topicName,partitions,replicationFactor);
    Set<String> topicListed = kafkaAdmin.listTopics();
    //we dond have solution to reset instance for each test
    assertEquals(kafkaBroker.getKafkaTestUtils().getTopicNames().size(), topicListed.size());
  }

  @Test
  void createPartitions() throws  Exception{

    String topicName = "partitions-created";
    int partitions = 3;
    int newpartions = 4;
    short replicationFactor = 1;
    KafkaTestUtils kafkaUtils = kafkaBroker.getKafkaTestUtils();
    NewTopic topic = new NewTopic(topicName,partitions,replicationFactor);
    kafkaAdmin.createTopic(topic);
    kafkaAdmin.createPartitions(topicName,newpartions);
    TopicDescription topicDescription =  kafkaUtils.describeTopic(topicName);
    assertEquals(newpartions, topicDescription.partitions().size());

  }

  @Test
  void alterConfigs() throws Exception {

    String topicName = "topic-to-be-updated";
    int partitions = 3;
    short replicationFactor = 1;
    String properties = "compression.type=producer,retention.ms=3600000";
    Map<String, String> data = PropertyUtil.stringToMap(properties);
    NewTopic topicCreated = new NewTopic(topicName,partitions,replicationFactor);
    kafkaAdmin.createTopic(topicCreated);
    Topic topic  = new Topic(topicName,partitions,replicationFactor,data,false);
    kafkaAdmin.alterConfigs(topic);
    Config config = kafkaAdmin.describeConfigs(topicName);
    Map<String, String> props = config.entries()
      .stream()
      .filter(x -> !x.isDefault() && !x.isReadOnly())
      .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
    Map.Entry<String, String> configEntry =  props.entrySet().iterator().next();
    assertEquals("compression.type", configEntry.getKey());
    assertEquals("producer", configEntry.getValue());
  }
}