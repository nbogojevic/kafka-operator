package nb.kafka.operator;

import java.util.Map;

public class Topic {
  private final String name;
  private final int partitions;
  private final short replicationFactor;
  private final Map<String, String> properties;
  private final boolean acl;

  public Topic(String name, int partitions, short replicationFactor, Map<String, String> properties, boolean acl) {
    super();
    this.name = name;
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
    this.properties = properties;    this.acl = acl;
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
