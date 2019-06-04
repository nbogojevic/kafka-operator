package nb.kafka.operator;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

public interface KafkaAdmin {
  int deleteTopic(String topicName) throws InterruptedException, ExecutionException;
  int createTopic(NewTopic topic) throws InterruptedException, ExecutionException;
  int numberOfBrokers() throws InterruptedException, ExecutionException;
  Config describeConfigs(String topicName) throws InterruptedException, ExecutionException;
  TopicDescription describeTopic(String topicName) throws InterruptedException, ExecutionException;
  Set<String> listTopics(int timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;
  Set<String> listTopics() throws InterruptedException, ExecutionException, TimeoutException;
  void createPartitions(String topicName, int nbPartitions) throws InterruptedException, ExecutionException;
  void alterConfigs(Topic topic) throws InterruptedException, ExecutionException;
}
