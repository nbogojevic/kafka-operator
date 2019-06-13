package nb.kafka.operator.importer;

/**
 * A simple interface for topic importer, i.e., classes that:
 * - list the set of existing kafka topics on the kafka cluster
 * - check which topics are not represented by a Kubernetes resource
 * - create the missing Kubernetes resources
 */
public interface TopicImporter {

  /**
   * Import the unmanaged Kafka topics as Kubernetes resources.
   */
  void importTopics();
}
