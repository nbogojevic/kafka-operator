package nb.kafka.operator.util;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;
import nb.kafka.operator.model.OperatorError;

public final class TopicValidator {
  private static final Logger log = LoggerFactory.getLogger(TopicValidator.class);

  private final AppConfig appConfig;
  private final Topic topic;

  public TopicValidator(AppConfig appConfig, Topic topic) {
    this.appConfig = appConfig;
    this.topic = topic;
  }

  public Set<OperatorError> validate() {
    Set<OperatorError> errors = new HashSet<>();
    errors.add(validateTopicName());
    errors.add(validateReplicationFactor());
    errors.add(validatePartitions());
    errors.add(validateRetentionMs());
    errors.remove(null);
    return errors;
  }

  public boolean isValid() {
    Set<OperatorError> errors = this.validate();
    if (!errors.isEmpty()) {
      errors.forEach(e -> log.error(e.toString()));
      return false;
    }
    return true;
  }

  public OperatorError validateTopicName() {
    return !TopicUtil.isValidTopicName(topic.getName()) ? OperatorError.NOT_VALID_TOPIC_NAME : null;
  }

  public OperatorError validateReplicationFactor() {
    return (topic.getReplicationFactor() > appConfig.getMaxReplicationFactor()) ? OperatorError.EXCEEDS_MAX_REPLICATION_FACTOR : null;
  }

  public OperatorError validatePartitions() {
    return (topic.getPartitions() > appConfig.getMaxPartitions()) ? OperatorError.EXCEEDS_MAX_PARTITIONS : null;
  }

  public OperatorError validateRetentionMs() {
    if (!topic.getProperties().isEmpty() && topic.getProperties().containsKey("retention.ms")) {
      int retentionMs = Integer.parseInt(topic.getProperties().get("retention.ms"));
      return (retentionMs > appConfig.getMaxRetentionMs()) ? OperatorError.EXCEEDS_MAX_RETENTION_MS : null;
    }
    return null;
  }
}
