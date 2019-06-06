package nb.kafka.operator.legacy;

import java.io.Closeable;
import java.util.List;

import nb.kafka.operator.Topic;

@Deprecated
public interface TopicResourceManager extends Closeable {
  List<Topic> get();
  void createResource(Topic topic);
  void watch();
  void close();
}