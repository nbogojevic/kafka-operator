package nb.kafka.operator;

import java.util.Map;
import java.util.Objects;

/**
 * Topic model.
 */
public class Topic {
  private final String name;
  private final int partitions;
  private final short replicationFactor;
  private final Map<String, String> properties;
  private final boolean acl;

  public Topic(String name, int partitions, short replicationFactor, Map<String, String> properties, boolean acl) {
    this.name = name;
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
    this.properties = properties;
    this.acl = acl;
  }

  public Topic(Topic topic) {
    this.name = topic.name;
    this.partitions = topic.partitions;
    this.replicationFactor = topic.replicationFactor;
    this.properties = topic.properties;
    this.acl = topic.acl;
  }

  public String getName() {
    return name;
  }

  public short getReplicationFactor() {
    return replicationFactor;
  }

  public int getPartitions() {
    return partitions;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public boolean isAcl() {
    return acl;
  }

  @Override
  public int hashCode() {
    return Objects.hash(acl, name, partitions, properties, replicationFactor);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Topic)) {
      return false;
    }
    Topic other = (Topic)obj;
    return acl == other.acl && Objects.equals(name, other.name) && partitions == other.partitions
        && Objects.equals(properties, other.properties) && replicationFactor == other.replicationFactor;
  }

  public Topic withProperties(Map<String, String> properties) {
    return new Topic(getName(), getPartitions(), getReplicationFactor(), properties, isAcl());
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
    builder.append(", acl=");
    builder.append(acl);
    builder.append(", properties=");
    builder.append(properties);
    builder.append("]");
    return builder.toString();
  }
}
