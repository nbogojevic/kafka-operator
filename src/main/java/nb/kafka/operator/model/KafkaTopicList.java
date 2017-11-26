package nb.kafka.operator.model;

import io.fabric8.kubernetes.client.CustomResourceList;

public class KafkaTopicList extends CustomResourceList<KafkaTopic> {
  private static final long serialVersionUID = -7509445634754526431L;
}