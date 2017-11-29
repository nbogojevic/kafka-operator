package nb.kafka.operator;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toMap;
import static nb.common.App.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class KafkaUtilities {

  private final static Logger log = LoggerFactory.getLogger(KafkaUtilities.class);

  private final short defaultReplFactor;
  
  private final Counter changedTopics;

  private final Counter createdTopics;

  private final Counter deletedTopics;

  private AdminClient adminClient;

  public KafkaUtilities(String kafkaUrl, String securityProtocol, short defaultReplFactor) {
    this.defaultReplFactor = defaultReplFactor;
    createdTopics = metrics().counter(MetricRegistry.name("created-topics"));
    changedTopics = metrics().counter(MetricRegistry.name("changed-topics"));
    deletedTopics = metrics().counter(MetricRegistry.name("deleted-topics"));

    Properties conf = new Properties();
    conf.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
    if (securityProtocol != null && !securityProtocol.trim().isEmpty()) {
      log.info("Using security protocol {}.", securityProtocol);      
      conf.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      conf.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");
      
    }
    adminClient = AdminClient.create(conf);
    topics();
  }
  
  public Set<String> topics() {
    KafkaFuture<Set<String>> names = adminClient.listTopics().names();
    try {
      KafkaFuture.allOf(names).get(10, TimeUnit.SECONDS);
      Set<String> topics = names.get();
      log.debug("Got topics: {}", topics);
      return topics;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IllegalStateException("Exception occured during topic retrieval.", e);
    }
  }
  
  public void deleteTopic(String name) {
    if (topics().contains(name)) {
      log.warn("Deleting topic. name: {}", name);
      DeleteTopicsResult result = adminClient.deleteTopics(singleton(name));
      try {
        result.all().get();
        log.warn("Deleted topic. name: {}, result: {}", name, result);
        deletedTopics.inc(result.values().size());
      } catch (InterruptedException | ExecutionException  e) {
        log.error("Exception occured during topic deletion. name: {}", name, e);
      }
    }
  }

  public boolean manageTopic(Topic topic) {
    log.debug("Requested update for {}", topic.getName() );
    if (topics().contains(topic.getName())) {
      updateTopic(topic);
      return true;
    } 
    NewTopic nt = new NewTopic(topic.getName(), partitionCount(topic), replicationFactor(topic));
    nt.configs(topic.getProperties());
    CreateTopicsResult ct = adminClient.createTopics(singleton(nt));
    try {
      ct.all().get();
      createdTopics.inc(ct.values().size());
      log.info("Created topic. name: {}, partitions: {}, replFactor: {}", topic.getName(), partitionCount(topic), replicationFactor(topic));
      return true;
    } catch (InterruptedException | ExecutionException  e) {
      if (e.getCause() != null && e.getCause() instanceof TopicExistsException) {
        log.debug("Topic exists. name {}", topic.getName());
        updateTopic(topic);
        return true;
      } else {
        log.info("Exception occured during topic creation. name {}", topic.getName(), e);
        return false;
      }
    }
  }
  
  private int partitionCount(Topic topic) {
    return topic.getPartitions() > 0 ? topic.getPartitions() : numberOfBrokers();
  }

  short replicationFactor(Topic topic) {
    return topic.getReplicationFactor() > 0 ? topic.getReplicationFactor() : defaultReplFactor;
  }

  public TopicWithParitions topic(String topicName) {
    TopicDescription topicDescription = topicDescription(topicName);
    List<TopicPartitionInfo> partitions = topicDescription.partitions();
    int maxReplicas = partitions.stream().map(p -> p.replicas().size()).max(naturalOrder()).orElse(0);
    int numPartitions = partitions.size();
    Map<String, String> configMap = topicConfiguration(topicName);
    return new TopicWithParitions(topicName, numPartitions, (short) maxReplicas, configMap, false, partitions);
  }

  public void setUpAclForUser(String username, Collection<String> consumedTopics, Collection<String> producedTopics) {
    Collection<AclBinding> existingAcls = userAcls(username);
    Collection<AclBinding> acls = new ArrayList<>(consumedTopics.size() + producedTopics.size());
    Collection<AclBinding> effectiveAcls = new ArrayList<>(consumedTopics.size() + producedTopics.size());
    String principal = "User:" + username;
    consumedTopics.forEach(t -> addAcl(acls, effectiveAcls, existingAcls, principal, t, AclOperation.READ));
    producedTopics.forEach(t -> addAcl(acls, effectiveAcls, existingAcls, principal, t, AclOperation.WRITE));
    try {
      adminClient.createAcls(acls).all().get();
      List<AclBindingFilter> aclToDelete = existingAcls.stream()
          .filter(b -> b.resource().resourceType() == ResourceType.TOPIC && (b.entry().operation() == AclOperation.READ || b.entry().operation() == AclOperation.WRITE))
          .filter(b -> !effectiveAcls.contains(b))
          .map(b -> new AclBindingFilter(
              new ResourceFilter(ResourceType.TOPIC, b.resource().name()), 
              new AccessControlEntryFilter(b.entry().principal(), b.entry().host(), b.entry().operation(), b.entry().permissionType())))
          .collect(Collectors.toList());
      if (!aclToDelete.isEmpty()) {
        try {
          adminClient.deleteAcls(aclToDelete).all().get();
        } catch (InterruptedException | ExecutionException e) {
          log.error("Unable to delete old ACL for {}, topics {}, {}.", username, consumedTopics, producedTopics, e);
        }
      }
    } catch (InterruptedException | ExecutionException e1) {
      log.error("Unable to delete set ACL for {}, topics {}, {}.", username, consumedTopics, producedTopics, e1);
    }
  }

  private void addAcl(Collection<AclBinding> acls, Collection<AclBinding> effectiveAcls, Collection<AclBinding> existingAcls, String principal, String topic, AclOperation operation) {
    AccessControlEntry ace = new AccessControlEntry(principal, "*", operation , AclPermissionType.ALLOW);
    AclBinding aclBinding = new AclBinding(new Resource(ResourceType.TOPIC, topic), ace);
    if (!existingAcls.contains(aclBinding)) {
      acls.add(aclBinding);
    }
    effectiveAcls.add(aclBinding);
  }

  private Collection<AclBinding> userAcls(String username) {
    try {
      AclBindingFilter any = new AclBindingFilter(
          new ResourceFilter(ResourceType.TOPIC, null),
          new AccessControlEntryFilter("User:" + username, null, AclOperation.ANY, AclPermissionType.ANY));    
      return adminClient.describeAcls(any).values().get();
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause() != null && e.getCause() instanceof SecurityDisabledException) {
        log.debug("Security disabled on server: {}", e.getCause().getMessage());
      } else {
        log.warn("Exception occured during user acls retrieval. name: {}", username, e);
      }
    }
    return Collections.emptyList();
  }

  private Map<String, String> topicConfiguration(String topicName) {
    Map<String, String> configMap = Collections.emptyMap();
    try {
      ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
      DescribeConfigsResult dc = adminClient.describeConfigs(singleton(cr));
      Map<ConfigResource, Config> configs = dc.all().get();
      Config config = configs.get(cr);
      configMap = config.entries().stream().filter(x -> !x.isDefault() && !x.isReadOnly()).collect(toMap(x -> x.name(), x -> x.value()));
      log.debug("Existing configuration for topic {} is {}", topicName, configMap);
    } catch (InterruptedException | ExecutionException e) {
      log.warn("Exception occured during topic configuration retrieval. name: {}", topicName, e);
    }
    return configMap;
  }

  TopicDescription topicDescription(String topicName) {
    DescribeTopicsResult dt = adminClient.describeTopics(singleton(topicName));
    try {
      dt.all().get();
      TopicDescription topicDescription = dt.values().get(topicName).get();
      return topicDescription;
    } catch (InterruptedException | ExecutionException  e) {
      if (e.getCause() != null && e.getCause() instanceof UnknownTopicOrPartitionException) {
        return null;
      }
      throw new IllegalStateException("Exception occured during topic details retrieval. name: " + topicName, e);
    }
  }

  private void updateTopic(Topic topic) {
    log.debug("Topic exists. name {}", topic.getName());
    TopicWithParitions oldTopic = topic(topic.getName());
    if (topic.getPartitions() > oldTopic.getPartitions()) {
      adminClient.createPartitions(singletonMap(topic.getName(), NewPartitions.increaseTo(topic.getPartitions())));
      changedTopics.inc();
      log.info("Updated topic. name: {}, new partitions: {}", topic.getName(), topic.getPartitions());
    } else if (topic.getPartitions() < oldTopic.getPartitions()) {
      log.warn("Unable to reduce number of partitions. name: {}, requested partitions: {}, original partitions {}",
               topic.getName(), topic.getPartitions(),  oldTopic.getPartitions());
    }
    if (topic.getReplicationFactor() != 0 && topic.getReplicationFactor() != oldTopic.getReplicationFactor()) {
      log.error("Replication factor change not supported. name: {}, requested replication-factor: {}, original replication-factor {}",
                topic.getName(), topic.getReplicationFactor(), oldTopic.getReplicationFactor());
    }
    if (topic.getProperties() == null || !topic.getProperties().equals(oldTopic.getProperties())) {
      log.info("Updating topic properties. name: {}, new properties: {}", topic.getName(), topic.getProperties());
      ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topic.getName());
      List<ConfigEntry> entries = new ArrayList<>();
      topic.getProperties().forEach((k, v) -> entries.add(new ConfigEntry(k.toString(), v.toString())));
      adminClient.alterConfigs(singletonMap(cr, new Config(entries)));
      changedTopics.inc();
    } else {
      log.debug("Topic properties are same. name: {}, new properties={}, old properties={}", topic.getName(), topic.getProperties(), oldTopic.getProperties());      
    }
  }

  private int numberOfBrokers() {
    try {
      return adminClient.describeCluster().nodes().get().size();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("Unable to get number of brokers.", e);
    }
  }

  static class TopicWithParitions extends Topic {
    final List<TopicPartitionInfo> partitions;

    public TopicWithParitions(String name, int numPartitions, short replicationFactor, Map<String, String> properties,
        boolean acl, List<TopicPartitionInfo> partitions) {
      super(name, numPartitions, replicationFactor, properties, acl);
      this.partitions = partitions;
    }    
  }
}
