package nb.kafka.operator.model;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class KafkaTopicDoneable extends CustomResourceDoneable<KafkaTopic > {
  public KafkaTopicDoneable(KafkaTopic resource, Function<KafkaTopic, KafkaTopic> function) {
    super(resource, function);
  }
}