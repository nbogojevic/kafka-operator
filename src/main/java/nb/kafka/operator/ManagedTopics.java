package nb.kafka.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ManagedTopics implements ManagedTopicsMXBean {
  private Map<String, Topic> managedTopics = new ConcurrentHashMap<>();

  @Override
  public List<Topic> getManagedTopics() {
    return new ArrayList<>(managedTopics.values());
  }

  void add(Topic topic) {
    managedTopics.put(topic.getName(), topic);
  }

  void delete(String topicName) {
    managedTopics.remove(topicName);
  }

  public int size() {
    return managedTopics.size();
  }
}
