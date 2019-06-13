package nb.kafka.operator.importer;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;
import nb.kafka.operator.TopicManager;
import nb.kafka.operator.model.KafkaTopic;
import nb.kafka.operator.model.KafkaTopicDoneable;
import nb.kafka.operator.model.KafkaTopicList;
import nb.kafka.operator.model.KafkaTopicSpec;
import nb.kafka.operator.util.KubernetesUtil;
import nb.kafka.operator.util.PropertyUtil;
import nb.kafka.operator.watch.TopicCrdWatcher;

/**
 * A topic importer that imports topic as custom resource definition of kind KafkaTopic.
 */
public class CrdImporter extends AbstractTopicImporter {
  private static final Logger log = LoggerFactory.getLogger(CrdImporter.class);

  private final KubernetesClient client;
  private CustomResourceDefinition crd;

  public CrdImporter(KubernetesClient client, TopicCrdWatcher watcher, TopicManager topicManager, AppConfig config) {
    super(watcher, topicManager, config);
    this.client = client;
    crd = KubernetesUtil.getTopicCrd(client, watcher.resourceKind());
  }

  /**
   * Create a custom resource definition from a topic model.
   * @param topic The topic model.
   */
  @Override
  protected void createTopicResource(Topic topic) {
    Map<String, String> labels = ((TopicCrdWatcher)topicWatcher()).labels();
    labels.put(GENERATOR_LABEL, appConfig().getOperatorId());
    Map<String, String> annotations = new HashMap<>();
    annotations.put(GENERATED_ANNOTATION, ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT));
    KafkaTopic item = new KafkaTopic();
    item.setMetadata(new ObjectMetaBuilder()
          .withName(KubernetesUtil.makeResourceName(topic.getName()))
          .withLabels(labels)
          .withAnnotations(annotations)
          .build());
    KafkaTopicSpec spec = new KafkaTopicSpec();
    spec.setName(topic.getName());
    spec.setPartitions(topic.getPartitions());
    spec.setProperties(PropertyUtil.propertiesAsString(topic.getProperties()));
    spec.setReplicationFactor(topic.getReplicationFactor());
    item.setSpec(spec);
    item = crdClient().create(item);
    log.info("Created KafkaTopic {} for topic {}", item, topic);
  }

  private MixedOperation<KafkaTopic, KafkaTopicList, KafkaTopicDoneable, Resource<KafkaTopic, KafkaTopicDoneable>>
      crdClient() {
    return client.customResource(crd, KafkaTopic.class, KafkaTopicList.class, KafkaTopicDoneable.class);
  }
}
