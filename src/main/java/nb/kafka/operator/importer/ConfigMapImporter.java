package nb.kafka.operator.importer;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;
import nb.kafka.operator.TopicManager;
import nb.kafka.operator.util.KubernetesUtil;
import nb.kafka.operator.util.PropertyUtil;
import nb.kafka.operator.watch.ConfigMapWatcher;

/**
 * A topic importer that imports topic as config maps.
 */
public class ConfigMapImporter extends AbstractTopicImporter {

  private static final Logger log = LoggerFactory.getLogger(ConfigMapImporter.class);

  private static final String PROPERTIES_KEY = "properties";
  private static final String REPLICATION_FACTOR_KEY = "replication-factor";
  private static final String PARTITIONS_KEY = "partitions";
  private static final String TOPIC_NAME_KEY = "name";
  private static final String ACL_KEY = "acl";

  private final KubernetesClient client;

  public ConfigMapImporter(KubernetesClient client, ConfigMapWatcher watcher, TopicManager topicManager,
      AppConfig config) {
    super(watcher, topicManager, config);
    this.client = client;
  }

  /**
   * Create a config map from a topic model.
   * @param topic The topic model.
   */
  @Override
  protected void createTopicResource(Topic topic) {
    ConfigMap cm = buildConfigMapResource(topic);
    cm = client.configMaps().create(cm);
    log.info("Created ConfigMap {} for topic {}", cm, topic);
  }

  protected ConfigMap buildConfigMapResource(Topic topic) {
    Map<String, String> data = new HashMap<>();
    data.put(TOPIC_NAME_KEY, String.valueOf(topic.getName()));
    data.put(PARTITIONS_KEY, String.valueOf(topic.getPartitions()));
    data.put(REPLICATION_FACTOR_KEY, String.valueOf(topic.getReplicationFactor()));
    data.put(PROPERTIES_KEY, PropertyUtil.propertiesAsString(topic.getProperties()));
    data.put(ACL_KEY, String.valueOf(topic.isAcl()));

    Map<String, String> labels = ((ConfigMapWatcher)topicWatcher()).labels();
    labels.put(GENERATOR_LABEL, appConfig().getOperatorId());

    String date = ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT);
    Map<String, String> annotations = Collections.singletonMap(GENERATED_ANNOTATION, date);
    return new ConfigMapBuilder()
        .withNewMetadata()
        .withName(KubernetesUtil.makeResourceName(topic.getName()))
        .withLabels(labels)
        .withAnnotations(annotations)
        .endMetadata()
        .withData(data)
        .build();
  }
}
