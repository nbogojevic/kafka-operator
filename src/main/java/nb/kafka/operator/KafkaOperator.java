package nb.kafka.operator;

import static nb.common.App.metrics;
import static nb.kafka.operator.util.PropertyUtil.getSystemPropertyOrEnvVar;
import static nb.kafka.operator.util.PropertyUtil.isBlank;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import io.undertow.util.StatusCodes;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import nb.common.App;
import nb.kafka.operator.importer.ConfigMapImporter;
import nb.kafka.operator.importer.TopicImporter;
import nb.kafka.operator.util.PropertyUtil;
import nb.kafka.operator.watch.ConfigMapWatcher;
import nb.kafka.operator.watch.TopicWatcher;

public class KafkaOperator {
  private static final Logger log = LoggerFactory.getLogger(KafkaOperator.class);

  public static final short DEFAULT_REPLICATION_FACTOR = 2;

  private final DefaultKubernetesClient kubeClient;
  private final TopicManager topicManager;
  private final TopicWatcher topicWatcher;
  private final TopicImporter topicImporter;
  private final AclManager aclManager;
  private final ManagedTopics managedTopics;

  public static void main(String[] args) {
    App.start("kafka.operator");
    AppConfig config = loadConfig();

    HealthServer.start(config);

    KafkaOperator operator = new KafkaOperator(config);
    if (config.isEnabledTopicImport()) {
      log.debug("Importing topics");
      operator.topicImporter.importTopics();
    }
    operator.watch();
    log.info("Operator {} started: Managing cluster {}", config.getOperatorId(), config.getKafkaUrl());
  }

  public static AppConfig loadConfig() {
    AppConfig config = new AppConfig();

    config.setKafkaUrl(getSystemPropertyOrEnvVar("bootstrap.server", "kafka:9092"));
    config.setOperatorId(getSystemPropertyOrEnvVar("operator.id", "kafka-operator"));
    config.setDefaultReplicationFactor(
        getSystemPropertyOrEnvVar("default.replication.factor", DEFAULT_REPLICATION_FACTOR));
    config.setEnableTopicImport(getSystemPropertyOrEnvVar("import.topics", true));
    config.setEnableAclManagement(getSystemPropertyOrEnvVar("enable.acl", false));
    config.setSecurityProtocol(getSystemPropertyOrEnvVar("security.protocol", ""));
    if (config.isEnabledAclManagement() && isBlank(config.getSecurityProtocol())) {
      config.setSecurityProtocol("SASL_PLAINTEXT");
      log.warn("ACL was enabled, but not security.protocol, forcing security protocol to {}",
          config.getSecurityProtocol());
    }
    config.setStandardLabels(PropertyUtil.stringToMap(getSystemPropertyOrEnvVar("standard.labels", "")));
    config.setStandardAclLabels(PropertyUtil.stringToMap(getSystemPropertyOrEnvVar("standard.acl.labels", "")));
    config
        .setUsernamePoolSecretName(getSystemPropertyOrEnvVar("username.pool.secret", "kafka-cluster-kafka-auth-pool"));
    config.setConsumedUsersSecretName(
        getSystemPropertyOrEnvVar("consumed.usernames.secret", "kafka-cluster-kafka-consumed-auth-pool"));

    log.debug("Loaded config, {}", config);
    return config;
  }

  public KafkaOperator(AppConfig config) {
    kubeClient = new DefaultKubernetesClient();

    KafkaAdmin ka = new KafkaAdminImpl(config.getKafkaUrl(), config.getSecurityProtocol());
    topicManager = new TopicManager(ka, config);
    aclManager = config.isEnabledAclManagement() ? new AclManager(kubeClient, config) : null;

    ConfigMapWatcher cmTopicWatcher = new ConfigMapWatcher(kubeClient, config);
    cmTopicWatcher.setOnCreateListener(this::createTopic);
    cmTopicWatcher.setOnUpdateListener(this::updateTopic);
    cmTopicWatcher.setOnDeleteListener(this::deleteTopic);
    topicWatcher = cmTopicWatcher;

    topicImporter = new ConfigMapImporter(kubeClient, cmTopicWatcher, topicManager, config);
    managedTopics = new ManagedTopics();
    App.registerMBean(managedTopics, "kafka.operator:type=ManagedTopics");
    metrics().register(MetricRegistry.name("managed-topics"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return managedTopics.size();
      }
    });
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  public DefaultKubernetesClient kubeClient() {
    return kubeClient;
  }

  private void shutdown() {
    //TODO handle graceful shutdown
    topicWatcher.close();
    if (aclManager != null) {
      aclManager.close();
    }
    kubeClient.close();
  }

  private void watch() {
    topicWatcher.watch();
    if (aclManager != null) {
      aclManager.watch();
    }
  }

  public void createTopic(Topic topic) {
    manageTopic(topic);
  }

  public void updateTopic(Topic topic) {
    manageTopic(topic);
  }

  private void manageTopic(Topic topic) {
    managedTopics.add(topic);

    log.debug("Requested update for {}", topic.getName());

    try {
      if (topicManager.listTopics().contains(topic.getName())) {
        doUpdateTopic(topic);
      } else {
        NewTopic nt = topicManager.createTopic(topic);
        log.info("Created topic. name: {}, partitions: {}, replFactor: {}, properties: {}", nt.name(),
            nt.numPartitions(), nt.replicationFactor(), topic.getProperties());
      }
    } catch (InterruptedException | ExecutionException | TimeoutException | TopicCreationException e) {
      log.error("Exception occured during topic creation. name {}", topic.getName(), e);
    } catch (TopicExistsException e) { // NOSONAR
      log.debug("Topic exists. name {}", topic.getName());
      doUpdateTopic(topic);
    }
  }

  public void deleteTopic(String topicName) {
    managedTopics.delete(topicName);
    try {
      topicManager.deleteTopic(topicName);
    } catch (InterruptedException | ExecutionException e) {
      log.error("Exception occured during topic deletion. name: {}", topicName, e);
    }
  }

  private void doUpdateTopic(Topic topic) {
    try {
      topicManager.updateTopic(topic);
    } catch (InterruptedException | ExecutionException e) {
      log.error("Exception occured during topic update. name {}", topic.getName(), e);
    }
  }
}
