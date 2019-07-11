package nb.kafka.operator.watch;

import static nb.kafka.operator.util.PropertyUtil.propertiesFromString;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;
import nb.kafka.operator.model.KafkaTopic;
import nb.kafka.operator.model.KafkaTopicDoneable;
import nb.kafka.operator.model.KafkaTopicList;
import nb.kafka.operator.model.KafkaTopicSpec;
import nb.kafka.operator.util.KubernetesUtil;

/**
 * A topic watcher that watches topic model represented by custom resource definition.
 * It will watch all CRDs of kind KafkaTopic.
 */
public class TopicCrdWatcher extends KubernetesWatcher<KafkaTopic> {
  private static final Logger log = LoggerFactory.getLogger(TopicCrdWatcher.class);

  private final CustomResourceDefinition crd;

  public TopicCrdWatcher(KubernetesClient client, AppConfig config) {
    super(client, KafkaTopic.class, config);
    crd = KubernetesUtil.getTopicCrd(client, resourceKind());
  }

  @Override
  public void watch() {
    log.debug("Watching {} for {} changes", resourceKind(), kubeClient().getNamespace());
    crdClient().withLabels(labels()).watch(this);
  }

  private MixedOperation<KafkaTopic, KafkaTopicList, KafkaTopicDoneable, Resource<KafkaTopic, KafkaTopicDoneable>>
      crdClient() {
    return kubeClient().customResource(crd, KafkaTopic.class, KafkaTopicList.class, KafkaTopicDoneable.class);
  }

  @Override
  protected Topic buildTopicModel(KafkaTopic resource) {
    try {
      KafkaTopicSpec spec = resource.getSpec();
      return new Topic(spec.getName(), spec.getPartitions(), spec.getReplicationFactor(),
          propertiesFromString(spec.getProperties()), spec.getAcl());
    } catch (IOException e) { // NOSONAR
      log.error("Unable to parse properties from {} {}", resourceKind(), resource.getMetadata().getName(), e);
      return null;
    }
  }

  @Override
  protected String getTopicName(KafkaTopic resource) {
    return resource.getSpec().getName();
  }

  @Override
  public List<Topic> listTopics() {
    KafkaTopicList list = crdClient().withLabels(labels()).list();
    return list.getItems()
        .stream()
        .map(this::buildTopicModel)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
