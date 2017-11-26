package nb.kafka.operator;

import java.io.Closeable;
import java.util.List;

public interface TopicManager extends Closeable {

  List<Topic> get();

  void createResource(Topic topic);

  void close();

  void watch();
}