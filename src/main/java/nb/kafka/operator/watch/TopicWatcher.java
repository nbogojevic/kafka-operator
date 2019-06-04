package nb.kafka.operator.watch;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import nb.kafka.operator.Topic;

public interface TopicWatcher extends Closeable {
  void watch();
  List<Topic> listTopics();

  default Set<String> listTopicNames() {
    return listTopics()
        .stream()
        .map(Topic::getName)
        .collect(Collectors.toSet());
  }

  default void close() {
  }

  void setOnCreateListener(Consumer<Topic> onCreate);
  void setOnUpdateListener(Consumer<Topic> onUpdate);
  void setOnDeleteListener(Consumer<String> onDelete);
}