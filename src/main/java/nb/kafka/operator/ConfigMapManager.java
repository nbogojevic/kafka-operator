package nb.kafka.operator;

import static nb.common.Config.getProperty;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import nb.common.Config;

public class ConfigMapManager implements Watcher<ConfigMap>, Supplier<List<Topic>>, Closeable {

  private final static Logger log = LoggerFactory.getLogger(ConfigMapManager.class);

  private DefaultKubernetesClient kubeClient;
  private KafkaUtilities kafkaUtils;

  public ConfigMapManager() {
    kubeClient = new DefaultKubernetesClient();
  }

  public List<Topic> get() {
    log.debug("Scanning {} for ConfigMaps.", kubeClient.getNamespace());
    ConfigMapList list = kubeClient.configMaps().withLabel("config", "kafka-topic").list();
    log.debug("Scanned {}", list);
    return list.getItems().stream().map(this::kafkaTopicBuilder).filter(Objects::nonNull)
                        .collect(Collectors.toList());
  }

  private Topic kafkaTopicBuilder(ConfigMap cm) {
    try {
      Properties props = new Properties();
      props.load(new StringReader(getProperty(cm.getData(), "properties", "")));

      Boolean deleted = getProperty(cm.getMetadata().getAnnotations(), "alpha.topic.kafka.nb/deleted", false);
      return new Topic(getProperty(cm.getData(), "name", cm.getMetadata().getName()), 
                       getProperty(cm.getData(), "num.partitions", 0),
                       getProperty(cm.getData(), "repl.factor", 0), 
                       Config.mapFromProperties(props), deleted);
    } catch (IOException e) {
      log.error("Unable to parse properties from configmap " + cm.getMetadata().getName(), e);
      return null;
    }
  }
  
  public void create(Topic topic) {
    Map<String, String> data = new HashMap<>();
    data.put("name", String.valueOf(topic.getName()));
    data.put("num.partitions", String.valueOf(topic.getPartitions()));
    data.put("repl.factor", String.valueOf(topic.getReplicationFactor()));
    try {
      Properties props = Config.propertiesFromMap(topic.getProperties());
      StringWriter sw = new StringWriter();
      props.store(sw, null);
      data.put("properties", sw.toString());
    } catch (IOException e) {
      log.error("This exception should not occur.", e);
    }
    Map<String, String> labels = new HashMap<>();
    labels.put("config", "kafka-topic");
    labels.put("app", "kafka-operator");
    Map<String, String> annotations = new HashMap<>();
    annotations.put("alpha.topic.kafka.nb/deleted", "false");
    annotations.put("alpha.topic.kafka.nb/generated", ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT));
    ConfigMap cm = new ConfigMapBuilder()
                        .withNewMetadata().withName(cleanName(topic.getName())).withLabels(labels).withAnnotations(annotations).endMetadata()
                        .withData(data).build();
    cm = kubeClient.configMaps().create(cm);
    log.info("Created ConfigMap {} for topic {}", cm, topic);    
  }

  private String cleanName(String name) {
    return name.replace('_', '-').toLowerCase();
  }

  @Override
  public void close() {
    log.info("Closing k8s communication.");
    kubeClient.close();
  }

  public void watch(KafkaUtilities kafkaUtils) {
    this.kafkaUtils = kafkaUtils;
    kubeClient.configMaps().withLabel("config", "kafka-topic").watch(this);
  }

  @Override
  public void eventReceived(Watcher.Action action, ConfigMap map) {
    switch (action) {
      case ADDED:
      case MODIFIED:
        kafkaUtils.manageTopic(kafkaTopicBuilder(map));
        break;
      case DELETED:
        kafkaUtils.deleteTopic(kafkaTopicBuilder(map).getName());
        break;
      case ERROR:
    }
  }

  @Override
  public void onClose(KubernetesClientException cause) {
    if (cause != null) {
      log.error("Exception while closing config map watch", cause);
    }
  }
}
