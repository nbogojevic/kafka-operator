package nb.kafka.operator;

import java.util.List;

public class ManagedTopics implements ManagedTopicsMXBean {
  private List<Topic> managedTopics;

  @Override
  public List<Topic> getManagedTopics() {
    return managedTopics;
  }

  public void setManagedTopics(List<Topic> managedTopics) {
    this.managedTopics = managedTopics;
  }
}
