package nb.kafka.operator.legacy;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.KafkaOperator;
import nb.kafka.operator.Topic;
import nb.kafka.operator.model.KafkaTopic;
import nb.kafka.operator.model.KafkaTopicDoneable;
import nb.kafka.operator.model.KafkaTopicList;
import nb.kafka.operator.model.KafkaTopicSpec;
import nb.kafka.operator.util.PropertyUtil;

@Deprecated
public class KafkaTopicCustomResourceManager extends AbstractKubernetesBasedManager<KafkaTopic> {
  private static final String CUSTOM_RESOURCE_VERSION = "v1alpha";

  private static final String CUSTOM_RESOURCE_GROUP = "nb";

  private static final String KAFKATOPIC_SINGULAR = "kafkatopic";

  private static final String KAFKATOPICS_PLURAL = "kafkatopics";

  private static final String CUSTOM_RESOURCE_DEFINITION_NAME = KAFKATOPICS_PLURAL + "." + CUSTOM_RESOURCE_GROUP;

  private static final Logger log = LoggerFactory.getLogger(KafkaTopicCustomResourceManager.class);

  private CustomResourceDefinition crd;

  private final AppConfig config;

  public KafkaTopicCustomResourceManager(KafkaOperator operator, AppConfig config) {
    super(KafkaTopic.class, operator, config.getStandardLabels());
    this.config = config;

    crd = kubeClient().customResourceDefinitions().withName(CUSTOM_RESOURCE_DEFINITION_NAME).get();
    if (crd == null) {
      crd = new CustomResourceDefinitionBuilder()
          .withNewMetadata().withName(CUSTOM_RESOURCE_DEFINITION_NAME).endMetadata()
          .withNewSpec()
          .withGroup(CUSTOM_RESOURCE_GROUP)
          .withVersion(CUSTOM_RESOURCE_VERSION)
          .withScope("Namespaced")
          .withNewNames()
            .withKind(resourceKind())
            .withPlural(KAFKATOPICS_PLURAL)
            .withSingular(KAFKATOPIC_SINGULAR)
          .endNames()
          .endSpec().build();

      crd = kubeClient().customResourceDefinitions().create(crd);
    }
  }

  private MixedOperation<KafkaTopic, KafkaTopicList, KafkaTopicDoneable, Resource<KafkaTopic, KafkaTopicDoneable>>
      getClient() {
    return kubeClient().customResource(crd, KafkaTopic.class, KafkaTopicList.class, KafkaTopicDoneable.class);
  }

  @Override
  public List<Topic> get() {
    KafkaTopicList list = getClient().withLabels(labels()).list();
    return list.getItems()
        .stream()
        .map(this::topicBuilder)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public void createResource(Topic topic) {
    Map<String, String> labels = labels();
    labels.put(GENERATOR_LABEL, config.getOperatorId());
    Map<String, String> annotations = new HashMap<>();
    annotations.put(GENERATED_ANNOTATION, ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT));
    KafkaTopic item = new KafkaTopic();
    item.setMetadata(new ObjectMetaBuilder()
          .withName(cleanName(topic.getName()))
          .withLabels(labels)
          .withAnnotations(annotations)
          .build());
    KafkaTopicSpec spec = new KafkaTopicSpec();
    spec.setName(topic.getName());
    spec.setPartitions(topic.getPartitions());
    spec.setProperties(PropertyUtil.propertiesAsString(topic.getProperties()));
    spec.setReplicationFactor(topic.getReplicationFactor());
    item.setSpec(spec);
    item = getClient().create(item);
    log.info("Created KafkaTopic {} for topic {}", item, topic);
  }

  @Override
  public void watch() {
    log.debug("Watcihing {} for {} changes", resourceKind(), kubeClient().getNamespace());
    getClient().withLabels(labels()).watch(this);

  }

  @Override
  protected Topic topicBuilder(KafkaTopic resource) {
    try {
      KafkaTopicSpec spec = resource.getSpec();
      return new Topic(spec.getName(), spec.getPartitions(), spec.getReplicationFactor(),
          PropertyUtil.propertiesFromString(spec.getProperties()), spec.getAcl());
    } catch (IOException e) {
      return null;
    }
  }
}
