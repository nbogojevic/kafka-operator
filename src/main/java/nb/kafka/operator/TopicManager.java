package nb.kafka.operator;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import nb.kafka.operator.model.OperatorError;
import nb.kafka.operator.util.TopicValidator;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicManager implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(TopicManager.class);

  private final KafkaAdmin kafkaAdmin;
  private final AppConfig config;

  public TopicManager(KafkaAdmin kafkaAdmin, AppConfig config) {
    this.config = config;
    this.kafkaAdmin = kafkaAdmin;
  }

  public NewTopic createTopic(Topic topic) throws TopicCreationException {
    try {
      NewTopic newTopic = new NewTopic(topic.getName(), partitionCount(topic), replicationFactor(topic));
      newTopic.configs(topic.getProperties());

      kafkaAdmin.createTopic(newTopic);
      log.info("Created topic. name: {}, partitions: {}, replFactor: {}", newTopic.name(), newTopic.numPartitions(),
          newTopic.replicationFactor());
      return newTopic;
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        throw (TopicExistsException)e.getCause();
      }
      throw new TopicCreationException(e);
    }
  }

  public void updateTopic(Topic newTopic) throws InterruptedException, ExecutionException {
    PartitionedTopic oldTopic = describeTopic(newTopic.getName());

    if(!new TopicValidator(config, newTopic, oldTopic).isValid())
      return;

    if (newTopic.getPartitions() > oldTopic.getPartitions()) {
      kafkaAdmin.createPartitions(newTopic.getName(), newTopic.getPartitions());
      log.info("Updated topic. name: {}, new partitions: {}", newTopic.getName(), newTopic.getPartitions());
    } else if (newTopic.getPartitions() == oldTopic.getPartitions()) {
      log.info("Unchanged topic partition number. name: {}, new partitions: {}", newTopic.getName(),
          newTopic.getPartitions());
    }

    if (newTopic.getProperties() == null || !newTopic.getProperties().equals(oldTopic.getProperties())) {
      kafkaAdmin.alterConfigs(newTopic);
      log.info("Updated topic properties. name: {}, new properties: {}", newTopic.getName(), newTopic.getProperties());
    } else {
      log.debug("Topic properties are same. name: {}, new properties={}, old properties={}", newTopic.getName(),
          newTopic.getProperties(), oldTopic.getProperties());
    }
  }

  public void deleteTopic(String topicName) throws InterruptedException, ExecutionException {
    kafkaAdmin.deleteTopic(topicName);
  }

  public PartitionedTopic describeTopic(String topicName) throws InterruptedException, ExecutionException {
    TopicDescription topicDescription = kafkaAdmin.describeTopic(topicName);
    List<TopicPartitionInfo> partitions = topicDescription.partitions();
    short maxReplicas = partitions.stream()
        .map(p -> p.replicas().size())
        .max(Comparator.naturalOrder())
        .orElse(0)
        .shortValue();
    int numPartitions = partitions.size();
    Map<String, String> configMap = getTopicConfiguration(topicName);
    return new PartitionedTopic(topicName, numPartitions, maxReplicas, configMap, false, partitions);
  }

  public Set<String> listTopics() throws InterruptedException, ExecutionException, TimeoutException {
    return kafkaAdmin.listTopics();
  }

  private int partitionCount(Topic topic) throws InterruptedException, ExecutionException {
    if (topic.getPartitions() > 0) {
      return topic.getPartitions();
    }
    return kafkaAdmin.numberOfBrokers();
  }

  private Map<String, String> getTopicConfiguration(String topicName) {
    Map<String, String> configMap = Collections.emptyMap();
    try {
      Config conf = kafkaAdmin.describeConfigs(topicName);
      configMap = conf.entries()
          .stream()
          .filter(x -> !x.isDefault() && !x.isReadOnly())
          .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
      log.debug("Existing configuration for topic {} is {}", topicName, configMap);
    } catch (InterruptedException | ExecutionException e) {
      log.warn("Exception occured during topic configuration retrieval. name: {}", topicName, e);
    }
    return configMap;
  }

  private short replicationFactor(Topic topic) {
    if (topic.getReplicationFactor() > 0) {
      return topic.getReplicationFactor();
    }
    return config.getDefaultReplicationFactor();
  }

  public static class PartitionedTopic extends Topic {
    private final List<TopicPartitionInfo> partitionInfos;

    public PartitionedTopic(String name, int numPartitions, short replicationFactor, Map<String, String> properties,
                            boolean acl, List<TopicPartitionInfo> partitionInfos) {
      super(name, numPartitions, replicationFactor, properties, acl);
      this.partitionInfos = Collections.unmodifiableList(partitionInfos);
    }
  }

  @Override
  public void close() {
    kafkaAdmin.close();
  }
}
