# kafka operator

Kafka operator is a process that automatically manages creation and deletion of kafka topics, their number of partitions, replicas as well as properties.  

## How does it work

Operator monitors ConfigMap Kubernetes resources that are tagged with `config=kafka-topic` label. From those ConfigMaps, operator extracts information
about kafka topic to create/update. This check is done periodically, and the period by default is every 60s.

## How to use kafka operator

Start kafka operator within a project. The simplest way to do it in OpenShift is as follows:

```bash
oc process -f src/main/openshift/kafka-operator-template.yaml | oc create -f -
```

If your zookeeper instance is not available on `zookeeper:2181` you should specify it when using `oc process`:

```bash
oc process -v ZOOKEEPER_BOOTSTRAP=yourzookeeperaddressandport -f src/main/openshift/kafka-operator-template.yaml | oc create -f -`.
```

Kubernetes should be configured to have only one instance of the operator running. Having several instances should not cause an issue, but will result in useless interactions with zookeeper and kafka cluster.

Operator must run under service account that that view rights on ConfigMaps in project where it runs. Zookeeper and Kafka clusters must be accessible from operator's namespace.

### Configuring kafka operator

Operator is configured using environment variables or system properties:

Environment variable | System property     | Description | Default value
---------------------|---------------------|-------------|--------------
ZOOKEEPER_BOOTSTRAP  | zookeeper.bootstrap | Address of zookeeper cluster | zookeeper:2181
DEFAULT_REPL_FACTOR  | default.repl.factor | Default replication factor to use when one is not specified in ConfigMap | 2
REFRESH_INTERVAL     | refresh.interval    | Interval in seconds between two actions of the operator | 60
ENABLE_TOPIC_DELETE  | enable.topic.delete | If set to true activates topic delete functionality | false
IMPORT_TOPICS        | import.topics | If set to true existing topics in kafka cluster will be imported and will be managed by operator | false

Following environment variables also influence behavior, but can't be set using openshift template as it is:

Environment variable | System property     | Description | Default value
---------------------|---------------------|-------------|--------------
JMX_METRICS | jmx.metrics | If set to true metrics are exposed as JMX beans | true
LOG_METRICS | log.metrics | If set to true activates logging metrics to console | false
LOG_METRICS_INTERVAL | log.metrics.interval | Interval in seconds of logging metrics to console  | 60
OPENTRACING_ENABLED | opentracing.enabled | Activates open-tracing experimental feature. | false


## How to manage kafka topics

Define a ConfigMap resource within a project where the operator is running. For example create file `my-kafka-topic.yaml` with following content:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-kafka-topic
  labels:
    config: kafka-topic  
data:
   num.partitions: 20
   repl.factor: 3
   properties: |
     retention.ms=1000000
```

Note that the name of file is not important. Once the file has been created, create the resource. For example, in openshift, do:

```bash
oc create -f my-kafka-topic.yaml
```

During next periodic check, the operator will read the ConfigMap and create a topic called `my-kafka-topic` with 20 partitions, replication factor of 3 and setting topic's `retention.ms` property to 1000000. Note that properties are represented as single ConfigMap key. If kafka topic contains capital characters or underscores, you can use `name` key in ConfigMap's `data` to specify correct name:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: underscore-kafka-topic
  labels:
    config: kafka-topic  
data:
   name: Underscore_Kafka_Topic
   num.partitions: 20
   repl.factor: 3
   properties: |
     retention.ms=1000000
```

Kafka operator only manages topics that are defined in ConfigMaps. If a ConfigMap for a topic is deleted, the operator will no longer manage it. On the other hand, if a new ConfigMap is created for an already existing topic, the operator will start managing it.

Default behavior:

* if `num.partitions` is not provided, the number of partitions will be set to number of brokers
* if `repl.factor` is not provided, the `DEFAULT_REPL_FACTOR` value is used.

#### Protected topics

The operator will not accept ConfigMaps that modify topics starting with double underscore `__`. Those are considered internal kafka topics.

### Modifying kafka topics

An existing kafka topic can be modified by updating its ConfigMap. For example we can modify number accepted compression method by updating our ConfigMap to:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-kafka-topic
  labels:
    config: kafka-topic  
data:
   num.partitions: 20
   repl.factor: 3
   properties: |
     retention.ms=1000000
     compression.type=producer
```


Known limitations:

* number of replicas can only be increased
* modifying replication factor is not supported at the moment

### Deleting kafka topics

To enable deletion of existing kafka topics, the operator must be run with `ENABLE_TOPIC_DELETE` set to `true` and topic deletion must be enabled in kafka cluster. If that is the case, a topic can be deleted by setting its ConfigMap's annotation `alpha.topic.kafka.nb/deleted` to `true`. For example to delete the topic we have defined, change its ConfigMap as follows:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-kafka-topic
  labels:
    config: kafka-topic  
  annotations:
    alpha.topic.kafka.nb/deleted: 'true'
 data:
   num.partitions: 20
   repl.factor: 3
   properties: |
     retention.ms=1000000
```

During next periodic check, the operator will delete the specified topic. Note that as with all other operations, this cannot be undone.

### Importing existing kafka topics

The operator can import existing kafka topics from brokers and start managing them. This is done by starting operator with environment variable `IMPORT_TOPICS` or system property `import.topics` set to `true`. This will import at startup all existing topics that don't start with double underscore (`__`). For each one, operator creates a ConfigMap with name that is same as the name of topic (with underscores replaced with dashes), and data content containing true name, number of partitions, replication factor and properties if any has been set. Its annotation `alpha.topic.kafka.nb/generated` is set the time of importing.

## TODO

* HOSA Metrics creation
* Kafka cluster creation
* Investigate using watch instead of poll
