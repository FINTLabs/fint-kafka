# FINT Kafka Project
[![CI](https://github.com/FINTLabs/fint-kafka/actions/workflows/ci.yaml/badge.svg)](https://github.com/FINTLabs/fint-kafka/actions/workflows/ci.yaml)

This library implements the Kafka patterns that we use in FINT:
* Entity
* Event
* Request/reply

It also has services to help ensure that all necessary topics is created and configured correctly.

## Topics
### Entity topic
```java
entityTopicService.ensureTopic(EntityTopicNameParameters
                .builder()
                .orgId(orgId)
                .domainContext("fint-core")
                .resource(String.format(
                                "%s-%s-%s",
                                adapterCapability.getDomain(),
                                adapterCapability.getPackageName(),
                                adapterCapability.getClazz()
                        )
                )
                .build(),
        retentionTimeMs);
```

### Event topic
```java
eventTopicService.ensureTopic(EventTopicNameParameters
                        .builder()
                        .orgId(adapterPing.getOrgId())
                        .domainContext("fint-core")
                        .eventName("adapter-health")
                        .build(),
                providerProperties.getAdapterPingRetentionTimeMs()
        );
```
