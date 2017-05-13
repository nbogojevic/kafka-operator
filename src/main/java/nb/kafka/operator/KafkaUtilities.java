package nb.kafka.operator;

import static java.util.Collections.singletonList;
import static java.util.Comparator.naturalOrder;
import static nb.common.App.metrics;
import static scala.collection.JavaConverters.asScalaBuffer;
import static scala.collection.JavaConverters.mapAsJavaMapConverter;
import static scala.collection.JavaConverters.seqAsJavaList;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicAndPartition;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import nb.common.Config;
import scala.collection.Seq;

public class KafkaUtilities {

  private final static Logger log = LoggerFactory.getLogger(KafkaUtilities.class);

  private final ZkUtils zkUtils;
  
  private final int defaultReplFactor;

  private final Counter changedTopics;

  private final Counter createdTopics;

  private final Counter deletedTopics;

  public KafkaUtilities(String url, int defaultReplFactor) {
    this.defaultReplFactor = defaultReplFactor;
    zkUtils = ZkUtils.apply(ZkUtils.createZkClient(url, 30000, 30000), false);
    createdTopics = metrics().counter(MetricRegistry.name("kafka-operator", "created-topics"));
    changedTopics = metrics().counter(MetricRegistry.name("kafka-operator", "changed-topics"));
    deletedTopics = metrics().counter(MetricRegistry.name("kafka-operator", "deleted-topics"));
  }
  
  public void deleteTopic(String name) {
    if (!AdminUtils.topicExists(zkUtils, name)) {
      log.warn("Deleting topic. name: {}", name);
      AdminUtils.deleteTopic(zkUtils, name);
      deletedTopics.inc();
    }
  }

  public void manageTopic(Topic kafkaTopic) {
    log.debug("Requested update for {}", kafkaTopic.getName() );
      KafkaTopicChangeRequest cr = new KafkaTopicChangeRequest(kafkaTopic);
      if (!AdminUtils.topicExists(zkUtils, cr.getName())) {
        try {
          AdminUtils.createTopic(zkUtils, cr.getName(), cr.getPartitions(), cr.getReplFactor(), cr.getProperties(), RackAwareMode.Enforced$.MODULE$);
          createdTopics.inc();
          log.info("Created topic. name: {}, partitions: {}, replFactor: {}", cr.getName(), cr.getPartitions(), cr.getReplFactor());
        } catch (TopicExistsException ignore) {
          log.debug("Topic exists. name {}", cr.getName());
          if (!cr.getProperties().equals(cr.getOldProperties())) {
            updateTopic(cr);
          }
        }
      } else {
        updateTopic(cr);
      }
  }

  public Topic topic(String topicName) {
    Map<TopicAndPartition, Seq<Object>> replicaAssigmentMap = replicaAssignementMap(topicName);
    int maxReplicas = replicaAssigmentMap.values().stream().map(a -> a.size()).max(naturalOrder()).orElse(0);
    int numPartitions = replicaAssigmentMap.size();
    Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topicName);
    return new Topic(topicName, numPartitions, maxReplicas, Config.mapFromProperties(props), false);
  }

  Map<TopicAndPartition, Seq<Object>> replicaAssignementMap(String topicName) {
    return mapAsJavaMapConverter(zkUtils.getReplicaAssignmentForTopics(asScalaBuffer(singletonList(topicName)).toList())).asJava();
  }

  private void updateTopic(KafkaTopicChangeRequest topic) {
    log.debug("Topic exists. name {}", topic.getName());
    if (topic.getPartitions() > topic.getOldPartitions()) {
      try {
        AdminUtils.addPartitions(zkUtils, topic.getName(), topic.getPartitions(), "", true, RackAwareMode.Enforced$.MODULE$);
        changedTopics.inc();
        log.info("Updated topic. name: {}, new partitions: {}", topic.getName(), topic.getPartitions());
      } catch (AdminOperationException e) {
        log.error("Failed to add partitions to topic. name: " + topic.getName(), e);
      }
    } else if (topic.getPartitions() < topic.getOldPartitions()) {
      log.warn("Unable to reduce number of partitions. name: {}, requested partitions: {}, original partitions {}",
               topic.getName(), topic.getPartitions(), topic.getOldPartitions());
    }
    if (topic.getReplFactor() != topic.getOldReplFactor()) {
      log.error("Replication factor change not supported. name: {}, requested replFactor: {}, original replFactor {}",
                topic.getName(), topic.getReplFactor(), topic.getOldReplFactor());
    }
    if (!topic.getProperties().equals(topic.getOldProperties())) {
      log.info("Updating topic properties. name: {}, new properties: {}", topic.getName(), topic.getProperties());
      AdminUtils.changeTopicConfig(zkUtils, topic.getName(), topic.getProperties());          
      changedTopics.inc();
    } else {
      log.debug("Topic properties are same. name: {}, p1={}, p2={}", topic.getName(), topic.getProperties(), topic.getOldProperties());      
    }
  }

  class KafkaTopicChangeRequest {
    private final Topic requested;
    private final Topic existing;

    KafkaTopicChangeRequest(Topic topic) {
      this.requested = topic; 

      this.existing = topic(topic.getName());
    }
    Properties getProperties() {
      Map<String, String> propMap = requested.getProperties();
      if (propMap == null) {
        propMap = existing.getProperties();
      } 
      return Config.propertiesFromMap(propMap);
    }
    int getReplFactor() {
      if (requested.getReplicationFactor() > 0) {
        return requested.getReplicationFactor();
      }
      return existing.getReplicationFactor() > 0 ? existing.getReplicationFactor() : defaultReplFactor;
    }
    String getName() {
      return requested.getName();
    }
    Properties getOldProperties() {
      return Config.propertiesFromMap(existing.getProperties());
    }
    int getOldPartitions() {
      return existing.getPartitions();
    }
    int getOldReplFactor() {
      return existing.getReplicationFactor();
    }
    int getPartitions() {
      if (requested.getPartitions() > 0) {
        return requested.getPartitions();
      }
      return existing.getPartitions() > 0 ? existing.getPartitions() : numberOfBrokers();
    }
  }

  private int numberOfBrokers() {
    return zkUtils.getAllBrokersInCluster().size();
  }

  public List<String> listTopics() {
    return seqAsJavaList(zkUtils.getAllTopics());
  }
}
