package nb.kafka.operator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartitionInfo;

public class PartitionedTopic extends Topic {
  private final List<TopicPartitionInfo> partitionInfos;

  public PartitionedTopic(String name, int numPartitions, short replicationFactor, Map<String, String> properties,
      boolean acl, List<TopicPartitionInfo> partitionInfos) {
    super(name, numPartitions, replicationFactor, properties, acl);
    this.partitionInfos = Collections.unmodifiableList(partitionInfos);
  }

  public List<TopicPartitionInfo> getPartitionInfos() {
    return partitionInfos;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Objects.hash(partitionInfos);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof PartitionedTopic)) {
      return false;
    }
    PartitionedTopic other = (PartitionedTopic)obj;
    return Objects.equals(partitionInfos, other.partitionInfos);
  }
}