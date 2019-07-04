package nb.kafka.operator;

import static nb.kafka.operator.util.PropertyUtil.isBlank;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAdminImpl implements KafkaAdmin {
  private static final Logger log = LoggerFactory.getLogger(KafkaAdminImpl.class);

  private final AppConfig config;
  private final AdminClient client;

  public KafkaAdminImpl(AppConfig config) {
    Properties conf = new Properties();
    conf.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    conf.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(config.getKafkaTimeoutMs()));
    if (!isBlank(config.getSecurityProtocol())) {
      log.info("Using security protocol {}.", config.getSecurityProtocol());
      conf.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, config.getSecurityProtocol());
      conf.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");
    }
    this.config = config;
    this.client = AdminClient.create(conf);
  }

  @Override
  public int deleteTopic(String topicName) throws InterruptedException, ExecutionException {
    DeleteTopicsResult result = client.deleteTopics(Collections.singleton(topicName));
    result.all().get();
    return result.values().size();
  }

  @Override
  public int createTopic(NewTopic topic) throws InterruptedException, ExecutionException {
    CreateTopicsResult result = client.createTopics(Collections.singleton(topic));
    result.all().get();
    return result.values().size();
  }

  @Override
  public int numberOfBrokers() throws InterruptedException, ExecutionException {
    return client.describeCluster().nodes().get().size();
  }

  @Override
  public Config describeConfigs(String topicName) throws InterruptedException, ExecutionException {
    ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    DescribeConfigsResult dc = client.describeConfigs(Collections.singleton(cr));
    Map<ConfigResource, Config> configs = dc.all().get();
    return configs.get(cr);
  }

  @Override
  public TopicDescription describeTopic(String topicName) throws InterruptedException, ExecutionException {
    DescribeTopicsResult dt = client.describeTopics(Collections.singleton(topicName));
    dt.all().get();
    return dt.values().get(topicName).get();
  }

  @Override
  public Set<String> listTopics() throws InterruptedException, ExecutionException, TimeoutException {
    KafkaFuture<Set<String>> names = client.listTopics().names();
    KafkaFuture.allOf(names).get();
    return names.get();
   }

  @Override
  public void createPartitions(String topicName, int nbPartitions) throws InterruptedException, ExecutionException {
    Map<String, NewPartitions> config = Collections.singletonMap(topicName, NewPartitions.increaseTo(nbPartitions));
    CreatePartitionsResult cr = client.createPartitions(config);
    cr.all().get();
  }

  @Override
  public void alterConfigs(Topic topic) throws InterruptedException, ExecutionException {
    ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topic.getName());
    List<ConfigEntry> entries = new ArrayList<>();
    topic.getProperties().forEach((k, v) -> entries.add(new ConfigEntry(k, v)));
    AlterConfigsResult ar = client.alterConfigs(Collections.singletonMap(cr, new Config(entries)));
    ar.all().get();
  }

  @Override
  public void close() {
    client.close(config.getKafkaTimeoutMs(), TimeUnit.MILLISECONDS);
  }
}
