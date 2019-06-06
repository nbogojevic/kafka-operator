package nb.kafka.operator.model;

import io.fabric8.kubernetes.client.CustomResource;

public class KafkaTopic extends CustomResource {
  private static final long serialVersionUID = 3436252994750725452L;
  private KafkaTopicSpec spec;

  public KafkaTopic() {
   setApiVersion("v1alpha");
   setKind("KafkaTopic");
  }

  public KafkaTopicSpec getSpec() {
    return this.spec;
  }
  public void setSpec(KafkaTopicSpec spec) {
    this.spec = spec;
  }
}