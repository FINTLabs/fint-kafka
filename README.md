# FINT Kafka Project
[![CI](https://github.com/FINTLabs/fint-kafka/actions/workflows/ci.yaml/badge.svg)](https://github.com/FINTLabs/fint-kafka/actions/workflows/ci.yaml)

This library implements the Kafka patterns that we use in FINT:
* Entity
* Event
* Request/reply

It also has services to help ensure that all necessary topics is created and configured correctly.

## Topics

### Generating topic names
Topic names are generated with the ``TopicNameParameters`` interface, which has four implementations:
* ``EntityTopicNameParameters``
* ``EventTopicNameParameters``
* ``RequestTopicNameParameters``
* ``ReplyTopicNameParameters``

The parameters are then used internally to generate a full topic name string, which follows the FINT topic naming standards.

#### Entity
##### Example
```
EntityTopicNameParameters
        .builder()
        .orgId(orgId)
        .domainContext("fint-core")
        .resource("arkiv.noark.administrativenhet")
        .build()
```
Result: ``<orgId>.fint-core.entity.arkiv.noark.administrativenhet``

#### Event
##### Example
```
EventTopicNameParameters
          .builder()
          .orgId(orgId)
          .domainContext("fint-core")
          .eventName("adapter-health")
          .build()
```
Result: ``<orgId>.fint-core.event.adapter-health``

#### Request
##### Example
```
RequestTopicNameParameters.builder()
        .orgId(orgId)
        .domainContext("skjema")
        .resource("arkiv.noark.sak")
        .parameterName("mappeid")
        .build()
```
Result: ``<orgId>.skjema.request.arkiv.noark.sak.by.mappeid``

#### Reply
##### Example:
```
ReplyTopicNameParameters.builder()
        .orgId(orgId)
        .domainContext("skjema")
        .applicationId(applicationId)
        .resource("arkiv.noark.sak")
        .build()
```
Result: ``<orgId>.skjema.reply.<applicationId>.arkiv.noark.sak``

  
### Ensuring topic creation and configuration
Ensuring topics are created and with a given configuration is done with the following services:
* ``EntityTopicService``
* ``EventTopicService``
* ``RequestTopicService``
* ``ReplyTopicService``

#### Entity
It is the responsibility of the service that **produces** the entities to ensure that the topic is created and has the correct configuration.

##### Example
```java
entityTopicService.ensureTopic(
        entityTopicNameParameters,
        retentionTimeMs
);
```

#### Event
It is the responsibility of the service that **produces** the events to ensure that the topic is created and has the correct configuration.

##### Example
```java
eventTopicService.ensureTopic(
        eventTopicNameParameters,
        retentionTimeMs
);
```

#### Request
For the request topic, it is the responsibility of the service that **consumes** the requests to ensure that the topic is created and has the correct configuration.

##### Example
```java
requestTopicService.ensureTopic(
        requestTopicNameParameters,
        retentionTimeMs
);
```

#### Reply
For the reply topic, it is the responsibility of the service that **produces** the requests to ensure that the topic is created and has the correct configuration.

##### Example
```java
replyTopicService.ensureTopic(
        replyTopicNameParameters,
        retentionTimeMs
);
```

## Producers
Producers are created with the following factories, that each create a specialized producer for the respective pattern:
* ``FintKafkaEntityProducerFactory`` -> ``EntityProducer<T>``
* ``FintKafkaEventProducerFactory`` -> ``EventProducer<T>``
* ``FintKafkaRequestProducerFactory`` -> ``RequestProducer<V, R>``

### Examples
#### Entity
```
fintKafkaEntityProducerFactory.createProducer(Object.class);
```
#### Event
```
fintKafkaEventProducerFactory.createProducer(SakResource.class)
```
#### Request
```
fintKafkaRequestProducerFactory.createProducer(
                replyTopicNameParameters,
                String.class,
                SakResource.class
);
```



## Consumers
Consumers are with the ``FintKafkaEntityConsumerFactory``, ``FintKafkaEventConsumerFactory`` and ``FintKafkaRequestConsumerFactory``. 

**NB:** The returned ``ConcurrentMessageListenerContainer<K, V>``has to be registered in the Spring context to be picked up by the Apache Kafka library. This is usually done by creating and returning the consumer in a @Bean annotated method in a configuration class.

### Error handling
Error handling is done with an optional parameter for the creation methods in the factories, which accepts implementations of the ``CommonErrorHandler`` interface. For simple logging of encountered errors, the ``CommonLoggingErrorHandler``can be used.


### Examples
#### Entity
```
entityConsumerFactory.createConsumer(
        entityTopicNameParameters,
        AdministrativEnhetResource.class,
        consumerRecord -> cache.put(
                ResourceLinkUtil.getSelfLinks(consumerRecord.value()),
                consumerRecord.value()
        ),
        new CommonLoggingErrorHandler()
);
```
#### Event
```
fintKafkaEventConsumerFactory.createConsumer(
        eventTopicNameParameters,
        Instance.class,
        consumerRecord -> instanceProcessingService.process(consumerRecord.value()),
        new CommonLoggingErrorHandler()
);
```
#### Request
```
fintKafkaRequestConsumerFactory.createConsumer(
        topicNameParameters,
        String.class,
        SakResource.class,
        (consumerRecord) -> fintClient
                .getResource("/arkiv/noark/sak/mappeid/" + consumerRecord.value(), SakResource.class)
                .block(),
        new CommonLoggingErrorHandler()
);
```

## Debugging and logging
To change the logging level of the Apache Kafka library, use the following property in the application properties:
```
logging:
  level:
    org.apache.kafka: INFO
```
