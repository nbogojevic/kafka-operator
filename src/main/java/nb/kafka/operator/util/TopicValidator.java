package nb.kafka.operator.util;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nb.kafka.operator.AppConfig;
import nb.kafka.operator.PartitionedTopic;
import nb.kafka.operator.Topic;
import nb.kafka.operator.model.OperatorError;

public final class TopicValidator {
  private static final Logger log = LoggerFactory.getLogger(TopicValidator.class);  

 
  private final AppConfig appConfig;
  private final Topic topic;
  private PartitionedTopic existingTopic;

  public TopicValidator(AppConfig appConfig, Topic topic, PartitionedTopic existingTopic) {
    this(appConfig, topic);
    this.existingTopic = existingTopic;
  }

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
    errors.add(validatePartitionsChange());
    errors.add(validateReplicationChange());
    errors.remove(null);
    return errors;
  }

  public boolean isValid() {
    Set<OperatorError> errors = this.validate();
    if (!errors.isEmpty()) {
      errors.forEach(e -> log.error(e.getErrorMessage()));
      return false;
    }
    return true;
  }

  protected OperatorError validateTopicName() {
    if (!TopicUtil.isValidTopicName(this.topic.getName())) {
      OperatorError error = OperatorError.NOT_VALID_TOPIC_NAME;
      String errorMessage = String.format(error.toString(), this.topic.getName());
      error.setErrorMessage(errorMessage);
      return error;
    }
    return null;
  }

  protected OperatorError validateReplicationFactor() {
    if (this.topic.getReplicationFactor() > this.appConfig.getMaxReplicationFactor()) {
      OperatorError error = OperatorError.EXCEEDS_MAX_REPLICATION_FACTOR;
      String errorMessage = String.format(error.toString(),
        this.topic.getName(), this.topic.getReplicationFactor(), appConfig.getMaxReplicationFactor());
      error.setErrorMessage(errorMessage);
      return error;
    }
    return null;
  }

  protected OperatorError validatePartitions() {
    if (this.topic.getPartitions() > this.appConfig.getMaxPartitions()) {
      OperatorError error = OperatorError.EXCEEDS_MAX_PARTITIONS;
      String errorMessage = String.format(error.toString(),
        this.topic.getName(), this.topic.getPartitions(), appConfig.getMaxPartitions());
      error.setErrorMessage(errorMessage);
      return error;
    }
    return null;
  }

  protected OperatorError validateRetentionMs() {
    if (this.topic.getProperties() != null && !this.topic.getProperties().isEmpty()
        && this.topic.getProperties().containsKey("retention.ms")) {
      int retentionMs = Integer.parseInt(this.topic.getProperties().get("retention.ms"));
      if ( retentionMs > this.appConfig.getMaxRetentionMs()) {
        OperatorError error = OperatorError.EXCEEDS_MAX_RETENTION_MS;
        String errorMessage = String.format(error.toString(), this.topic.getName(), retentionMs, appConfig.getMaxRetentionMs());
        error.setErrorMessage(errorMessage);
        return error;
      }
    }
    return null;
  }

  protected OperatorError validatePartitionsChange() {
    if (this.existingTopic != null &&
      this.topic.getPartitions() < this.existingTopic.getPartitions()) {
      OperatorError error = OperatorError.PARTITIONS_REDUCTION_NOT_ALLOWED;
      String errorMessage = String.format(error.toString(),
        this.topic.getPartitions(), this.existingTopic.getPartitions());
      error.setErrorMessage(errorMessage);
      return error;
    }
    return null;
  }

  protected OperatorError validateReplicationChange() {
    if (this.existingTopic != null &&
            this.topic.getReplicationFactor() != 0 &&
            this.topic.getReplicationFactor() != this.existingTopic.getReplicationFactor()) {
      OperatorError error = OperatorError.REPLICATION_FACTOR_CHANGE_NOT_SUPPORTED;
      String errorMessage = String.format(error.toString(),
        this.topic.getReplicationFactor(), this.existingTopic.getReplicationFactor());
      error.setErrorMessage(errorMessage);
      return error;
    }
    return null;
  }
}
