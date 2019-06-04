package nb.kafka.operator.util;

public final class TopicUtil {
  private TopicUtil() {
  }

  /**
   * Checks if a topic name is not a reserved Kafka name.
   * @param topicName The topic name.
   * @return true if the topic name is valid, false otherwise.
   */
  public static boolean isValidTopicName(String topicName) {
    return !topicName.startsWith("__");
  }
}
