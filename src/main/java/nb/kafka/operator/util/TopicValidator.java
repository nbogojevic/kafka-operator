package nb.kafka.operator.util;

import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;
import nb.kafka.operator.model.OperatorError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;

public final class TopicValidator {
  private static final Logger log = LoggerFactory.getLogger(TopicValidator.class);

  AppConfig appConfig;
  Topic topic;


  public TopicValidator(AppConfig appConfig, Topic topic) {
    this.appConfig = appConfig;
    this.topic = topic;
  }

  public ArrayList<OperatorError> validate(){
    ArrayList<OperatorError> errors = new ArrayList<>();
    errors.add(validateTopicName());
    errors.add(validateReplicationFactor());
    errors.add(validatePartitions());
    errors.add(validateRetentionMs());
    errors.removeAll(Collections.singleton(null));
    return errors;
  }

  public boolean isValid() {
    ArrayList<OperatorError> errors = this.validate();
    if (!errors.isEmpty()) {
      errors.forEach( e -> log.error(e.toString()));
      return false;
    }
    return true;
  }

  public OperatorError validateTopicName() {
    return !TopicUtil.isValidTopicName(this.topic.getName()) ? OperatorError.NOT_VALID_TOPIC_NAME : null;
  }

  public OperatorError validateReplicationFactor() {
    return (this.topic.getReplicationFactor() > this.appConfig.getMaxReplicationFactor()) ? OperatorError.EXCEEDS_MAX_REPLICATION_FACTOR : null;
  }

  public OperatorError validatePartitions() {
    return (this.topic.getPartitions() > this.appConfig.getMaxPartitions()) ? OperatorError.EXCEEDS_MAX_PARTITIONS : null;
  }

  public OperatorError validateRetentionMs() {
    if (!topic.getProperties().isEmpty() && topic.getProperties().containsKey("retention.ms")) {
      int retentionMs = Integer.parseInt(this.topic.getProperties().get("retention.ms"));
      return ( retentionMs > this.appConfig.getMaxRetentionMs()) ? OperatorError.EXCEEDS_MAX_RETENTION_MS : null;
    }
    return null;
  }
}
