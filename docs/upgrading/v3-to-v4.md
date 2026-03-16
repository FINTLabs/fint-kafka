# Oppgradering: `v3` -> `v4`

> Eksakte tagger for dette hoppet: `v3.2.0-rc-3 -> v4.0.3`.

Denne guiden beskriver hva som må endres i applikasjoner som bruker biblioteket.

## 1. Hurtigoversikt

| Område | Endring |
|---|---|
| Consumer factories | `createFactory(...)` -> `createRecordConsumerFactory(...)` |
| Nye consumer-muligheter | Batch-consumer via `createBatchConsumerFactory(...)` |
| Error handler-konfig | `CommonErrorHandler` i konfig -> `DefaultErrorHandler` |
| Consumer-parametre | Nye felter: `ackMode`, `maxPollIntervalMs`, `maxPollRecords` |
| Konfigvariabler | Tre `fint.kafka.*message-size*`-variabler er fjernet |

## 2. API-endringer for consumere

### 2.1 Factory-metoder er renamed

`v3`:

```java
eventConsumerFactoryService.createFactory(
        EventPayload.class,
        record -> handle(record.value()),
        EventConsumerConfiguration.empty()
);
```

`v4`:

```java
eventConsumerFactoryService.createRecordConsumerFactory(
        EventPayload.class,
        record -> handle(record.value()),
        EventConsumerConfiguration.empty()
);
```

Dette gjelder tilsvarende for:

- `EntityConsumerFactoryService`
- `EventConsumerFactoryService`
- `ErrorEventConsumerFactoryService`
- `RequestConsumerFactoryService`

### 2.2 Batch-consumer er ny i `v4`

```java
eventConsumerFactoryService.createBatchConsumerFactory(
        EventPayload.class,
        records -> handleBatch(records),
        EventConsumerConfiguration.empty()
);
```

### 2.3 Error handler-type i konfigurasjon er endret

- I `v3`: konfig-felt med type `CommonErrorHandler`
- I `v4`: konfig-felt med type `DefaultErrorHandler`

## 3. Nye consumer-parametre i `v4`

Nye felter i consumer-konfigurasjonene:

- `ackMode`
- `maxPollIntervalMs`
- `maxPollRecords`

Eksempel:

```java
EventConsumerConfiguration configuration = EventConsumerConfiguration
        .builder()
        .ackMode(ContainerProperties.AckMode.BATCH)
        .maxPollIntervalMs(15000)
        .maxPollRecords(500)
        .build();
```

For request-consumer kom også ny klasse i `v4`: `RequestConsumerConfiguration`.

## 4. Konfigurasjonsendringer

### 4.1 Fjernede variabler

- `fint.kafka.producer-max-message-size`
- `fint.kafka.consumer-max-message-size`
- `fint.kafka.consumer-partition-fetch-bytes`

### 4.2 Variabler som fortsatt brukes

- `fint.kafka.application-id`
- `fint.kafka.enable-ssl`
- `fint.kafka.default-retention-time-ms`
- `fint.kafka.default-replicas`
- `fint.kafka.default-partitions`
- `fint.kafka.default-cleanup-policy`
- `fint.kafka.topic.org-id`
- `fint.kafka.topic.domain-context`

## 5. Dependency/plattformnotat

Bibliotekets egen BOM-base er endret mellom taggene (`Spring Boot 3.1.3` -> `2.7.18` i bibliotekets build). Verifiser Spring/Kafka-kompatibilitet i konsumerende applikasjoner.

## 6. Sjekkliste

1. Bytt alle `createFactory(...)` til `createRecordConsumerFactory(...)`.
2. Ta i bruk `createBatchConsumerFactory(...)` der batch er ønsket.
3. Oppdater error-handler-bruk til `DefaultErrorHandler` der den settes i konfig.
4. Fjern avhengighet til de tre fjernede `fint.kafka.*message-size*`-variablene.
