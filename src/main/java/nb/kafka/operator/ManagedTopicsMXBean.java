package nb.kafka.operator;

import java.util.List;

public interface ManagedTopicsMXBean {
  List<Topic> getManagedTopics();
}
