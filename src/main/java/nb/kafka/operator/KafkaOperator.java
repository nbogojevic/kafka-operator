package nb.kafka.operator;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.Gauge;
import nb.kafka.operator.importer.ConfigMapImporter;
import nb.kafka.operator.importer.TopicImporter;
import nb.kafka.operator.util.MeterManager;
import nb.kafka.operator.watch.ConfigMapWatcher;
import nb.kafka.operator.watch.TopicWatcher;

public class KafkaOperator {
  private static final Logger log = LoggerFactory.getLogger(KafkaOperator.class);

  public static final short DEFAULT_REPLICATION_FACTOR = 2;

  private final KubernetesClient kubeClient;
  private final TopicManager topicManager;
  private final TopicWatcher topicWatcher;
  private final TopicImporter topicImporter;
  private final AclManager aclManager;
  private final MeterManager meterManager;
  private final AppConfig config;
  private final ManagedTopicList managedTopics;

  private State operatorState;

  public KafkaOperator(AppConfig config) {
    this(config, new DefaultKubernetesClient(), new KafkaAdminImpl(config.getKafkaUrl(), config.getSecurityProtocol()));
  }

  public KafkaOperator(AppConfig config, KubernetesClient kubeClient, KafkaAdmin kafkaAdmin) {
    this(config, kubeClient, kafkaAdmin, new ConfigMapWatcher(kubeClient, config), MeterManager.defaultMeterManager());
  }

  public KafkaOperator(AppConfig config, KubernetesClient kubeClient, KafkaAdmin kafkaAdmin, ConfigMapWatcher watcher,
      MeterManager meterManager) {
    try {
      this.config = config;
      this.kubeClient = kubeClient;

      this.topicManager = new TopicManager(kafkaAdmin, config);
      this.aclManager = config.isEnabledAclManagement() ? new AclManager(meterManager, kubeClient, config) : null;

      watcher.setOnCreateListener(this::createTopic);
      watcher.setOnUpdateListener(this::updateTopic);
      watcher.setOnDeleteListener(this::deleteTopic);
      this.topicWatcher = watcher;

      this.topicImporter = new ConfigMapImporter(kubeClient, watcher, topicManager, config);

      this.meterManager = meterManager;
      this.managedTopics = new ManagedTopicList(meterManager, config, topicWatcher.listTopics());

      this.operatorState = State.CREATED;
      meterManager.register(Gauge.builder("operator.state", operatorState::ordinal));
    } catch (Throwable t) {
      this.operatorState = State.FAILED;
      throw t;
    }
  }

  public KubernetesClient kubeClient() {
    return kubeClient;
  }

  public void shutdown() {
    try {
      topicWatcher.close();
      if (aclManager != null) {
        aclManager.close();
      }
      meterManager.close();
      kubeClient.close();
    } finally {
      operatorState = State.STOPPED;
    }
  }

  public void watch() {
    try {
      topicWatcher.watch();
      if (aclManager != null) {
        aclManager.watch();
      }
      operatorState = State.RUNNING;
    } catch (Throwable t) {
      operatorState = State.FAILED;
      throw t;
    }
  }

  public void createTopic(Topic topic) {
    manageTopic(topic);
  }

  public void updateTopic(Topic topic) {
    manageTopic(topic);
  }

  public void importTopics() {
    topicImporter.importTopics();
  }

  private void manageTopic(Topic topic) {
    log.debug("Requested update for {}", topic.getName());

    try {
      if (topicManager.listTopics().contains(topic.getName())) {
        doUpdateTopic(topic);
      } else {
        NewTopic nt = topicManager.createTopic(topic);
        managedTopics.add(topic);
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
    if (!config.isEnabledTopicDelete()) {
      return;
    }

    try {
      topicManager.deleteTopic(topicName);
      managedTopics.delete(topicName);
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

  public enum State {
    CREATED, RUNNING, STOPPED, FAILED
  }

  public State getState() {
    return operatorState;
  }
}
