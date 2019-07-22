[![Build Status](https://travis-ci.org/nbogojevic/kafka-operator.svg?branch=master)](https://travis-ci.org/nbogojevic/kafka-operator)
[![Code Coverage](https://codecov.io/github/nbogojevic/kafka-operator/coverage.svg)](https://codecov.io/gh/nbogojevic/kafka-operator)

# Kafka operator

Kafka operator is a process that automatically manages creation and deletion of kafka topics, their number of partitions, replicas as well as properties.  

## How does it work

Operator monitors ConfigMap Kubernetes resources that are tagged with `config=kafka-topic` label. From those ConfigMaps, operator extracts information about kafka topic to create/update. This check is by watching all changes to ConfigMaps with the this label.

## How to use kafka operator

To be able to run the operator, you first need to get the needed kubernetes/openshift resources. It is enough to build the project using the Maven profile `fabric8` which uses fabric8 pulgin to generate these resources:

```bash
mvn clean install -Pfabric8
```

The manifests `kubernetes.yml` and `openshift.yml` should be localted in `target/classes/META-INF/fabric8/`

Start kafka operator within a project. The simplest way to do it in Kubernetes is as follows:

```bash
kubectl apply -f target/classes/META-INF/fabric8/kubernetes.yml
```

Start kafka operator within a project. The simplest way to do it in OpenShift is as follows:

```bash
oc apply -f target/classes/META-INF/fabric8/openshift.yml
```

If your kafka instance is not available on `kafka:9092` you should modify value of BOOTSTRAP_SERVER environment variable.

Kubernetes should be configured to have only one instance of the operator running per kafka cluster. Having several instances should not cause an issue, but will result in useless interactions with cluster.

Operator must run under service account that that view rights on ConfigMaps in project where it runs. Kafka clusters must be accessible from operator's namespace.

### Configuring kafka operator

Operator is configured using environment variables or system properties:

Environment variable | System property     | Description | Default value
---------------------|---------------------|-------------|--------------
BOOTSTRAP_SERVERS     | bootstrap.servers | Address of kafka cluster | kafka:9092
DEFAULT_REPLICATION_FACTOR | default.replication.factor | Default replication factor to use when one is not specified in ConfigMap | 2
ENABLE_TOPIC_DELETE        | enable.topic.delete | When set to true and a configMap is deleted then the operator will delete the associated topic in the kafka cluster | false
ENABLE_TOPIC_IMPORT        | enable.topic.import	 | I	f set to true existing topics in kafka cluster will be imported and will be managed by operator | false
ENABLE_ACL           | enable.acl | If set to true activates acl management | false
LOG_LEVEL            | LOG_LEVEL | Set log level (debug|info|warn|error) | info
STANDARD_LABELS      | standard.labels| Comma-separated list of labels that must be set on ConfigMap that are taken into account| empty list
STANDARD_ACL_LABELS  | standard.acl.labels| Comma-separated list of labels that must be set on deployments that are taken into account| empty list
USERNAME_POOL_SECRET | username.pool.secret| Name of the secret containing pool of available usernames | kafka-cluster-kafka-auth-pool
CONSUMED_USERNAMES_SECRET | consumed.usernames.secret| Name of the secret containing list of already used usernames | kafka-cluster-kafka-consumed-auth-pool
SECURITY_PROTOCOL    | security.protocol | Security protocol to use SASL_SSL or SASL_PLAINTEXT. | empty
OPERATOR_ID          | operator.id| Unique id of the operator in a namespace | kafka-operator
KAFKA_TIMEOUT_MS          | kafka.timeout.ms | Unique id of the operator in a namespace | 30000
METRICS_PORT          | metrics.port | HTTP port to expose metrics | 9889
HEALTHS_PORT          | healths.port | HTTP port for health check endpoints | 9559
MAX_REPLICATION_FACTOR          | max.replication.factor | The maximum allowed value of replication factor | 3
MAX_PARTITIONS          | max.partitions | The maximum allowed value of topic partitions | 2000
MAX_RETENTION_MS          | max.retention.ms | The maximum allowed value of topic retention in Ms | 604800000 (7 days)

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
  num.partitions: "20"
  repl.factor: "3"
  properties: |
    retention.ms=1000000
```

Note that the name of file is not important. Once the file has been created, create the resource. For example, in openshift, do:

```bash
kubectl create -f my-kafka-topic.yaml
```

On ConfigMap change, the operator will read the ConfigMap and create a topic called `my-kafka-topic` with 20 partitions, replication factor of 3 and setting topic's `retention.ms` property to 1000000. Note that properties are represented as single ConfigMap key. If kafka topic contains capital characters or underscores, you can use `name` key in ConfigMap's `data` to specify correct name:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: underscore-kafka-topic
  labels:
    config: kafka-topic  
data:
  name: Underscore_Kafka_Topic
  num.partitions: "20"
  repl.factor: "3"
  properties: |
    retention.ms=1000000
```

Kafka operator only manages topics that are defined in ConfigMaps. If a ConfigMap for a topic is deleted, the operator will delete it and no longer manage it. On the other hand, if a new ConfigMap is created for an already existing topic, the operator will start managing it.

Default behavior:

* if `num.partitions` is not provided, the number of partitions will be set to number of brokers
* if `repl.factor` is not provided, the `DEFAULT_REPLICATION_FACTOR` value is used.

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
  num.partitions: "20"
  repl.factor: "3"
  properties: |
    retention.ms=1000000
    compression.type=producer
```


Known limitations:

* number of replicas can only be increased
* modifying replication factor is not supported at the moment

### Deleting kafka topics

To delete existing kafka topic managed by operator, just delete its ConfigMap.

### Importing existing kafka topics

The operator can import existing kafka topics from brokers and start managing them. This is done by starting operator with environment variable `IMPORT_TOPICS` or system property `import.topics` set to `true`. This will import at startup all existing topics that don't start with double underscore (`__`). For each one, operator creates a ConfigMap with name that is same as the name of topic (with underscores replaced with dashes), and data content containing true name, number of partitions, replication factor and properties if any has been set. Its annotation `topic.kafka.nb/generated` is set to the time of importing.

## Managing ACLs

Operator can watch for Deployments or DeploymentConfigs that have label `kafka-operator` set to `inject-credentials`. It will read following
annotations and create secret if needed.
* `topic.kafka.nb/consumes-topics` is the comma-separated list of topics that are consumed by Deployment. Those topics will be readable by user
assigned to Deployment.
* `topic.kafka.nb/produces-topics` is the comma-separated list of topics that are produced by Deployment. Those topics will be writeable by user
assigned to Deployment.
* `topic.kafka.nb/topic-secret` is the name of secret that is used by Deployment. The secret will be generated if not present and will contain
an allocated user, password, JAAS configuration file and UR to kafka bootstrap server. 

```yaml
bootstrap.server: "kafka:9092" 
kafka-client-jaas.conf: |
  KafkaClient {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username=EG8m6ceyaONFJMg0
   password=5vdmIFGZ4sMoeElU;
 };
username: "EG8m6ceyaONFJMg0"
password: "5vdmIFGZ4sMoeElU" 
```

TODO: Explain using initializers

## Authentication

Kafka operator can use SASL_PLAINTEXT to authenticate itself when connecting to kafka cluster. See kafka documentation for details on how to provide credentials.
When using ACL management, the operator will by default activate authentication.

## Using SonarQube locally
This project use the [SonarQube](https://www.sonarqube.org) inspector. You can setup a local instance in a few minutes: https://docs.sonarqube.org/latest/setup/get-started-2-minutes. 

Once the SonarQube local server is running (`http://localhost:9000`, configurable in `pom.xml`), use `mvn sonar:sonar` to trigger analyses.
