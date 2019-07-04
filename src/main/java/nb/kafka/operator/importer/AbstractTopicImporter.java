package nb.kafka.operator.importer;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;
import nb.kafka.operator.TopicManager;
import nb.kafka.operator.util.PropertyUtil;
import nb.kafka.operator.util.TopicUtil;
import nb.kafka.operator.watch.TopicWatcher;

/**
 * Skeleton class for topic importer that implements a common import logic. The
 * {@link AbstractTopicImporter#createTopicResource(Topic)} must be implemented depending on the type of Kubernetes
 * resource to create.
 */
public abstract class AbstractTopicImporter implements TopicImporter {
  private static final Logger log = LoggerFactory.getLogger(AbstractTopicImporter.class);

  public static final String GENERATED_ANNOTATION = PropertyUtil.kubeAnnotation("generated");
  public static final String GENERATOR_LABEL = "generator";
  public static final String KAFKA_OPERATOR_GENERATOR = "kafka-operator";

  private final AppConfig config;
  private final TopicWatcher watcher;
  private final TopicManager topicManager;

  public AbstractTopicImporter(TopicWatcher watcher, TopicManager topicManager, AppConfig config) {
    this.config = config;
    this.watcher = watcher;
    this.topicManager = topicManager;
  }

  /*
   * (non-Javadoc)
   * @see TopicImporter#importTopics()
   */
  @Override
  public void importTopics() {
    try {
      Set<String> existingTopics = topicManager.listTopics();
      Set<String> excludedTopics = watcher.listTopicNames();
      existingTopics.removeAll(excludedTopics);

      for (String topicName : existingTopics) {
        if (TopicUtil.isValidTopicName(topicName)) {
          doCreateResource(topicName);
        }
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.error("Exception while importing topics", e);
    }
  }

  private void doCreateResource(String topicName) {
    try {
      createTopicResource(topicManager.describeTopic(topicName));
    } catch (InterruptedException | ExecutionException e) {
      log.error("Exception while importing topic. name = {}", topicName, e);
    }
  }

  /**
   * Create the relevant Kubernetes resource from a topic model.
   * @param topic The topic model.
   */
  protected abstract void createTopicResource(Topic topic);

  public AppConfig appConfig() {
    return config;
  }

  public TopicWatcher topicWatcher() {
    return watcher;
  }

  public TopicManager topicManager() {
    return topicManager;
  }
}
