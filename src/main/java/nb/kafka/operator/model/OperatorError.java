package nb.kafka.operator.model;

public enum OperatorError {

  KAFKA_UNREACHABLE(1, "Kafka cluster is unreachable at {%s}"),
  EXCEEDS_MAX_REPLICATION_FACTOR(2, "The configMap {%s} has a repl.factor {%d} which exceeds the max value of {%d}"),
  EXCEEDS_MAX_PARTITIONS(3, "The configMap {%s} has a num.partitions {%d} which exceeds the max value of {%d}"),
  EXCEEDS_MAX_RETENTION_MS(4,"The configMap {%s} has a retention.ms {%d} which exceeds the max value of {%d} ms"),
  NOT_VALID_TOPIC_NAME(5, "The topic name {%s} is not valid as it should not start with '__'"),
  PARTITIONS_REDUCTION_NOT_ALLOWED(6, "Reduction of the number of partitions from {%d} to {%d} is not allowed"),
  REPLICATION_FACTOR_CHANGE_NOT_SUPPORTED(7, "Change of the replication factor from {%d} to {%d} is not supported");

  private int code;
  private String description;
  private String errorMessage;

  private OperatorError(int code, String description){
    this.code = code;
    this.description = description;
  }

  public int getCode() {
    return this.code;
  }

  public String getErrorMessage() {
    return this.errorMessage;
  }

  public void setErrorMessage(String message) {
    this.errorMessage = message;
  }

  @Override
  public String toString() {
    return this.name() + " [" + this.code + "]: " + this.description;
  }
}
