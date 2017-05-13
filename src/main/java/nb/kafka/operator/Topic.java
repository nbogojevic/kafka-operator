package nb.kafka.operator;

import java.util.Map;

public class Topic {
  private final String name;
  private final int partitions;
  private final int replicationFactor;
  private final Map<String, String> properties;
  private final boolean deleted;
  
  public Topic(String name, int partitions, int replicationFactor, Map<String, String> properties, boolean deleted) {
    super();
    this.name = name;
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
    this.properties = properties;
    this.deleted = deleted;
  }

  public String getName() {
    return name;
  }
  
  public int getReplicationFactor() {
    return replicationFactor;
  }

  public int getPartitions() {
    return partitions;
  }
  
  public Map<String, String> getProperties() {
    return properties;
  }

  public boolean isDeleted() {
    return deleted;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TopicDescriptor [name=");
    builder.append(name);
    builder.append(", partitions=");
    builder.append(partitions);
    builder.append(", replicationFactor=");
    builder.append(replicationFactor);
    builder.append(", deleted=");
    builder.append(deleted);
    builder.append(", properties=");
    builder.append(properties);
    builder.append("]");
    return builder.toString();
  }
}
