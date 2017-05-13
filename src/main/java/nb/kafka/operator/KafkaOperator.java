package nb.kafka.operator;

import static java.util.stream.Collectors.toList;
import static nb.common.Config.getSystemPropertyOrEnvVar;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nb.common.App;

public class KafkaOperator {
  private final static Logger log = LoggerFactory.getLogger(KafkaOperator.class);

  private final KafkaUtilities kafkaUtils;
  private final boolean enabledDelete;
  Set<String> previousExistingTopics;

  private final ManagedTopics managedTopics;

  private final ConfigMapManager configMapManager;

  public KafkaOperator(KafkaUtilities kafkaUtils, ConfigMapManager configMapManager, boolean enabledDelete) {
    this.kafkaUtils = kafkaUtils;
    this.configMapManager = configMapManager;
    this.enabledDelete = enabledDelete;
    previousExistingTopics = Collections.emptySet();
    managedTopics = new ManagedTopics();
    App.registerMBean(managedTopics, "kafka-operator:type=ManagedTopics");
  }

  public void checkAndApplyChanges() {
    try {
      log.debug("Operator wake-up.");
      LinkedHashSet<String> existingTopics = new LinkedHashSet<>(kafkaUtils.listTopics());
      List<Topic> requestedTopics = configMapManager.get();
      // Filter protected topics
      requestedTopics = requestedTopics.stream().filter(this::notProtected).collect(toList());
      managedTopics.setManagedTopics(requestedTopics);
            
      requestedTopics.stream().filter(t -> !t.isDeleted()).forEach(kafkaUtils::manageTopic);
      if (enabledDelete) {
        requestedTopics.stream().filter(Topic::isDeleted).map(Topic::getName).forEach(kafkaUtils::deleteTopic);
      }
      filterManagedTopics(existingTopics, requestedTopics);
      if (!previousExistingTopics.equals(existingTopics)) {
        log.info("Topic(s) not under operator control: {}", existingTopics);
      }
      previousExistingTopics = existingTopics;
    } catch (Exception e) {
      log.error("Exception during operator wake-up", e);
      throw e;
    }
  }

  
  private boolean notProtected(Topic td) {
    return notProtected(td.getName());
  }

  private boolean notProtected(String topicName) {
    return !topicName.startsWith("__");
  }

  public void importTopics() {
    LinkedHashSet<String> existingTopics = new LinkedHashSet<>(kafkaUtils.listTopics());
    List<Topic> requestedTopics = configMapManager.get();
    filterManagedTopics(existingTopics, requestedTopics);
    existingTopics.stream().filter(this::notProtected).map(kafkaUtils::topic).forEach(configMapManager::create);    
  }

  private static void filterManagedTopics(LinkedHashSet<String> existingTopics, List<Topic> requestedTopics) {
    existingTopics.removeAll(requestedTopics.stream().map(Topic::getName).collect(toList()));
  }
  
  public void shutdown() {
    if (configMapManager instanceof Closeable) {
      try {
        ((Closeable) configMapManager).close();
      } catch (IOException e) {
        log.error("Exception while closing topic supplier.", e);
      }
    }
  }

  public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    App.start("kafka-operator");
    String kafkaUrl = getSystemPropertyOrEnvVar("zookeeper.bootstrap", "zookeeper:2181");
    int defaultReplFactor = getSystemPropertyOrEnvVar("default.repl.factor", 2);
    int refreshInterval = getSystemPropertyOrEnvVar("refresh.interval", 60);
    boolean enableDelete = getSystemPropertyOrEnvVar("enable.topic.delete", false);
    boolean importTopics = getSystemPropertyOrEnvVar("import.topics", false);
    
    KafkaOperator operator = new KafkaOperator(new KafkaUtilities(kafkaUrl, defaultReplFactor), new ConfigMapManager(), enableDelete);
    
    if (importTopics) {
      operator.importTopics();
    }
    log.info("Starting operator. Wake-up every {} second(s).", refreshInterval);  
    ScheduledThreadPoolExecutor operatorExecutor = new ScheduledThreadPoolExecutor(1) {
      protected void afterExecute(Runnable r, Throwable t) {
        if (t != null) {
          log.error("Exception occured during wake-up execution.", t);
        }
      }
    };
    Runtime.getRuntime().addShutdownHook(new Thread(operator::shutdown));
    operatorExecutor.scheduleWithFixedDelay(operator::checkAndApplyChanges, 0, refreshInterval, TimeUnit.SECONDS);
  }
}
