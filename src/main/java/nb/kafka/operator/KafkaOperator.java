package nb.kafka.operator;

import static java.util.stream.Collectors.toList;
import static nb.common.App.metrics;
import static nb.common.Config.getSystemPropertyOrEnvVar;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import nb.common.App;
import nb.common.Config;

public class KafkaOperator {
  private final static Logger log = LoggerFactory.getLogger(KafkaOperator.class);

  private final DefaultKubernetesClient kubeClient;
  private final KafkaUtilities kafkaUtils;
  private final String operatorId;
  private final String kafkaUrl;
  private final TopicManager topicManager;
  private final AclManager aclManager;
  private final ManagedTopics managedTopics;

  private KafkaOperator(String operatorId, String kafkaUrl, short defaultReplFactor, Map<String, String> standardLabels, boolean enableAcl, 
      String usernamePoolSecretName, String consumedUsersSecretName, Map<String, String> standardAclLabels) {
    this.kafkaUrl = kafkaUrl;
    this.operatorId = operatorId != null ? operatorId : "kafka-operator";
    this.kafkaUtils = new KafkaUtilities(kafkaUrl, defaultReplFactor);
    this.kubeClient = new DefaultKubernetesClient();
    this.topicManager = new ConfigMapManager(this, standardLabels);
    this.aclManager = enableAcl ? new AclManager(this, usernamePoolSecretName, consumedUsersSecretName, standardAclLabels) : null;

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
  
  private void importTopics() {
    Set<String> existingTopics = kafkaUtils.topics();
    List<Topic> requestedTopics = topicManager.get();
    filterManagedTopics(existingTopics, requestedTopics);
    existingTopics.stream().filter(this::notProtected).map(kafkaUtils::topic).forEach(topicManager::createResource);    
  }

  private static void filterManagedTopics(Set<String> existingTopics, List<Topic> requestedTopics) {
    existingTopics.removeAll(requestedTopics.stream().map(Topic::getName).collect(toList()));
  }
  
  private void shutdown() {
    topicManager.close();
    if (aclManager != null) {
      aclManager.close();
    }
    kubeClient.close();
  }
  
  public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException {
    App.start("kafka.operator");
    String kafkaUrl = getSystemPropertyOrEnvVar("bootstrap.server", "kafka:9092");
    String operatorId = getSystemPropertyOrEnvVar("operator.id", "kafka-operator");
    short defaultReplFactor = getSystemPropertyOrEnvVar("default.replication.factor", (short) 2);
    boolean importTopics = getSystemPropertyOrEnvVar("import.topics", true);
    boolean enableAcl = getSystemPropertyOrEnvVar("enable.acl", false);
    Map<String, String> standardLabels = Config.stringToMap(getSystemPropertyOrEnvVar("standard.labels", ""));
    Map<String, String> aclLabels = Config.stringToMap(getSystemPropertyOrEnvVar("standard.acl.labels", ""));
    String usernamePoolSecret = getSystemPropertyOrEnvVar("username.pool.secret", "kafka-cluster-kafka-auth-pool");
    String consumedUserSecret = getSystemPropertyOrEnvVar("consumed.usernames.secret", "kafka-cluster-kafka-consumed-auth-pool");
    KafkaOperator operator = new KafkaOperator(operatorId, kafkaUrl, defaultReplFactor, standardLabels, enableAcl, usernamePoolSecret, consumedUserSecret, aclLabels);
    if (importTopics) {
      log.debug("Importing topics from cluster {}.", kafkaUrl);
      operator.importTopics();
    }
    operator.watch();
    log.info("Operator {} started: Managing cluster {}.", operatorId, kafkaUrl);
  }

  private void watch() {
    topicManager.watch();
    if (aclManager != null) {
      aclManager.watch();
    }
  }

  public boolean notProtected(String topicName) {
    return !topicName.startsWith("__");
  }

  public void manageTopic(Topic topic) {
    managedTopics.add(topic);
    kafkaUtils.manageTopic(topic); 
  }

  public void deleteTopic(String topicName) {
    managedTopics.delete(topicName);
    kafkaUtils.deleteTopic(topicName); 
  }

  public String getBootstrapServer() {
    return kafkaUrl;
  }

  public String getGeneratorId() {
    return operatorId;
  }

  public KafkaUtilities kafkaUtils() {
    return kafkaUtils;
  }
}
