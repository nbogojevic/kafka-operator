package nb.kafka.operator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.Gauge;
import nb.kafka.operator.util.MeterManager;

/**
 * A list of managed topics and the associated meters.
 */
public class ManagedTopicList {
  private final AppConfig config;
  private final MeterManager meterManager;
  private final Map<String, Topic> managedTopics = new ConcurrentHashMap<>();
  private final Map<String, Gauge> managedTopicGauges = new ConcurrentHashMap<>();

  private final AtomicInteger topicsCounter = new AtomicInteger();

  public ManagedTopicList(MeterManager meterMgr, AppConfig config, List<Topic> initialTopics) {
    this.config = config;
    this.meterManager = meterMgr;

    // gauge for the number of managed topics
    meterManager.register(Gauge.builder("managed.topics", topicsCounter::get)
      .description("Number of managed topics")
      .tag("operator-id", config.getOperatorId()));

    // create a gauge for all initial topics
    addAll(initialTopics);
  }

  void addAll(Collection<Topic> topics) {
    for (Topic topic : topics) {
      add(topic);
    }
  }

  void add(Topic topic) {
    managedTopics.put(topic.getName(), topic);
    topicsCounter.set(managedTopics.size());

    if (!managedTopicGauges.containsKey(topic.getName())) {
      Gauge gauge = meterManager.register(Gauge.builder("managed.topic", () -> 1)
          .tag("operator-id", config.getOperatorId())
          .tag("topic-name", topic.getName()));
      managedTopicGauges.put(topic.getName(), gauge);
    }
  }

  void delete(String topicName) {
    if (managedTopicGauges.containsKey(topicName)) {
      Gauge gauge = managedTopicGauges.remove(topicName);
      meterManager.close(gauge);
    }

    managedTopics.remove(topicName);
    topicsCounter.set(managedTopics.size());
  }

  public int size() {
    return managedTopics.size();
  }
}
