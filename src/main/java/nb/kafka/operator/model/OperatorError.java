package nb.kafka.operator.model;

public enum OperatorError {

  UNKNOWN(1, "Unknown"),
  KAFKA_UNREACHABLE(2, "Kafka cluster is unreachable at {%s}"),
  EXCEEDS_KAFKA_TIMEOUT(3, "The operation {%s} has timed out on Kafka cluster at {%s}"),
  EXCEEDS_MAX_REPLICATION_FACTOR(4, "The configMap {%s} has a replication-factor {%d} which exceeds the max value of {%d}"),
  EXCEEDS_MAX_PARTITIONS(5, "The configMap {%s} has a number of partitions {%d} which exceeds the max value of {%d}"),
  EXCEEDS_MAX_RETENTION_MS(6,"The configMap {%s} has a retention.ms {%d} which exceeds the max value of {%d} ms"),
  NOT_VALID_TOPIC_NAME(7,"The topic name {%s} is not valid as it should not start with '__'");

  private int code;
  private String description;

  OperatorError(int code, String description){
    this.code = code;
    this.description = description;
  }

  public int getCode() {
    return this.code;
  }

  public String getDescription() {
    return this.description;
  }

  @Override
  public String toString() {
    return String.format("%s [%s]: %s", this.name(), this.code, this.description);
  }
}
