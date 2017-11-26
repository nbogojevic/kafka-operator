package nb.kafka.operator.model;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.fabric8.kubernetes.api.model.KubernetesResource;

@SuppressWarnings("rawtypes")
@JsonDeserialize(using = JsonDeserializer.None.class)
public class KafkaTopicSpec implements KubernetesResource {
  private static final long serialVersionUID = -2976660651367253201L;
  
  private String name;
  private short replicationFactor;
  private int partitions;
  private String properties;
  private boolean acl;
  
  public String getName() {
    return name;
  }
  public short getReplicationFactor() {
    return replicationFactor;
  }
  public int getPartitions() {
    return partitions;
  }
  public String getProperties() {
    return properties;
  }
  public void setName(String name) {
    this.name = name;
  }
  public void setReplicationFactor(short replicationFactor) {
    this.replicationFactor = replicationFactor;
  }
  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }
  public void setProperties(String properties) {
    this.properties = properties;
  }
  public boolean getAcl() {
    return acl;
  }
  public void setAcl(boolean  acl) {
    this.acl = acl;
  }
}