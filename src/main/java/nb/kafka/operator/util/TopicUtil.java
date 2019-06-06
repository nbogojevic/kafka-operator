package nb.kafka.operator.util;

public final class TopicUtil {
  private TopicUtil() {
  }

  public static boolean isValidTopicName(String topicName) {
    return !topicName.startsWith("__");
  }
}
