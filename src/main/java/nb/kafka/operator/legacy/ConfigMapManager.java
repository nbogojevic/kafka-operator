package nb.kafka.operator.legacy;

import static nb.kafka.operator.util.PropertyUtil.getProperty;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.KafkaOperator;
import nb.kafka.operator.Topic;
import nb.kafka.operator.util.PropertyUtil;

@Deprecated
public class ConfigMapManager extends AbstractKubernetesBasedManager<ConfigMap> {

  private static final String PROPERTIES_KEY = "properties";

  private static final String REPLICATION_FACTOR_KEY = "replication-factor";

  private static final String PARTITIONS_KEY = "partitions";

  private static final String TOPIC_NAME_KEY = "name";

  private static final String ACL_KEY = "acl";

  private static final String KAFKA_TOPIC_LABEL_VALUE = "kafka-topic";

  private static final String CONFIG_LABEL = "config";

  private static final Logger log = LoggerFactory.getLogger(ConfigMapManager.class);

  private final AppConfig config;

  public ConfigMapManager(KafkaOperator operator, AppConfig config) {
    super(ConfigMap.class, operator, config.getStandardLabels());
    this.config = config;
  }

  @Override
  public List<Topic> get() {
    log.debug("Scanning {} for ConfigMaps.", kubeClient().getNamespace());
    ConfigMapList list = kubeClient().configMaps().withLabels(labels()).list();
    log.debug("Scanned {}", list);
    return list.getItems()
        .stream()
        .map(this::topicBuilder)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  protected Topic topicBuilder(ConfigMap cm) {
    try {
      return new Topic(getProperty(cm.getData(), TOPIC_NAME_KEY, cm.getMetadata().getName()),
                       getProperty(cm.getData(), PARTITIONS_KEY, 0),
                       getProperty(cm.getData(), REPLICATION_FACTOR_KEY, (short)0),
                       PropertyUtil.propertiesFromString(getProperty(cm.getData(), PROPERTIES_KEY, "")),
                       getProperty(cm.getData(), ACL_KEY, false));
    } catch (IOException e) {
      log.error("Unable to parse properties from ConfigMap {}", cm.getMetadata().getName(), e);
      return null;
    }
  }

  @Override
  public void createResource(Topic topic) {
    Map<String, String> data = new HashMap<>();
    data.put(TOPIC_NAME_KEY, String.valueOf(topic.getName()));
    data.put(PARTITIONS_KEY, String.valueOf(topic.getPartitions()));
    data.put(REPLICATION_FACTOR_KEY, String.valueOf(topic.getReplicationFactor()));
    data.put(PROPERTIES_KEY, PropertyUtil.propertiesAsString(topic.getProperties()));
    data.put(ACL_KEY, String.valueOf(topic.isAcl()));
    Map<String, String> labels = labels();
    labels.put(GENERATOR_LABEL, config.getOperatorId());

    String date = ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT);
    Map<String, String> annotations = Collections.singletonMap(GENERATED_ANNOTATION, date);
    ConfigMap cm = new ConfigMapBuilder()
                        .withNewMetadata()
                        .withName(cleanName(topic.getName()))
                        .withLabels(labels)
                        .withAnnotations(annotations)
                        .endMetadata()
                        .withData(data).build();
    cm = kubeClient().configMaps().create(cm);
    log.info("Created ConfigMap {} for topic {}", cm, topic);
  }

  @Override
  public void watch() {
    log.debug("Watching {} for ConfigMap changes", kubeClient().getNamespace());
    watch = kubeClient().configMaps().withLabels(labels()).watch(this);
  }

  protected Map<String, String> labels() {
    Map<String, String> labels = super.labels();
    labels.put(CONFIG_LABEL, KAFKA_TOPIC_LABEL_VALUE);
    return labels;
  }
}
