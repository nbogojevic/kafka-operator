package nb.kafka.operator.watch;

import static nb.kafka.operator.util.PropertyUtil.getProperty;
import static nb.kafka.operator.util.PropertyUtil.propertiesFromString;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import nb.kafka.operator.AppConfig;
import nb.kafka.operator.Topic;

/**
 * A topic watcher that watches topic model represented by config maps.
 * It will watch all config maps having the label config=kafka-topic
 */
public class ConfigMapWatcher extends KubernetesWatcher<ConfigMap> {

  private static final String PROPERTIES_KEY = "properties";
  private static final String REPLICATION_FACTOR_KEY = "replication-factor";
  private static final String PARTITIONS_KEY = "partitions";
  private static final String TOPIC_NAME_KEY = "name";
  private static final String ACL_KEY = "acl";
  private static final String KAFKA_TOPIC_LABEL_VALUE = "kafka-topic";
  private static final String CONFIG_LABEL_KEY = "config";

  private static final Logger log = LoggerFactory.getLogger(ConfigMapWatcher.class);

  private Watch watch;

  public ConfigMapWatcher(KubernetesClient client, AppConfig config) {
    super(client, ConfigMap.class, config);
  }

  @Override
  protected Topic buildTopicModel(ConfigMap cm) {
    try {
      return new Topic(getProperty(cm.getData(), TOPIC_NAME_KEY, cm.getMetadata().getName()),
                       getProperty(cm.getData(), PARTITIONS_KEY, -1),
                       getProperty(cm.getData(), REPLICATION_FACTOR_KEY, (short)-1),
                       propertiesFromString(getProperty(cm.getData(), PROPERTIES_KEY, "")),
                       getProperty(cm.getData(), ACL_KEY, false));
    } catch (IOException e) {
      log.error("Unable to parse properties from ConfigMap {}", cm.getMetadata().getName(), e);
      return null;
    }
  }

  @Override
  public void watch() {
    log.debug("Watching {} for ConfigMap changes", kubeClient().getNamespace());
    watch = kubeClient().configMaps().withLabels(labels()).watch(this);
  }

  @Override
  public Map<String, String> labels() {
    Map<String, String> labels = super.labels();
    labels.put(CONFIG_LABEL_KEY, KAFKA_TOPIC_LABEL_VALUE);
    return labels;
  }

  @Override
  public List<Topic> listTopics() {
    log.debug("Scanning {} for ConfigMaps.", kubeClient().getNamespace());
    ConfigMapList list = kubeClient().configMaps().withLabels(labels()).list();
    log.debug("Scanned {}", list);
    return list.getItems()
        .stream()
        .map(this::buildTopicModel)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public void close() {
    if (watch != null) {
      watch.close();
    }
  }
}
