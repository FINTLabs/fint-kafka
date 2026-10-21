<!-- Midlertidig dokument for review. PR-en som inneholder denne filen skal ikke merges. -->

# fint-kafka v7: før og etter

Med v7 «Balansert» blir et typisk Kafka-oppsett i en app til ett topic-navn, én fabrikk og en lambda. Kotlin og Java bruker det samme API-et.

Eksemplene viser forslaget fra v7-planen. Ingenting er implementert ennå, så navnene på v7-klassene kan endres og har derfor ingen lenker.

- **v6-eksemplene** er forenklede utdrag av ekte kode i Novari-repoer. Kilden er lenket under hvert eksempel.
- **Lenker til fint-kafka** peker på commit [`3b6bff5`](https://github.com/FINTLabs/fint-kafka/tree/3b6bff5089df2d7b8b23168848367a88b2af8248) på `main` (v6).

## Innhold

1. [Event-producer med topic-opprettelse](#1-event-producer-med-topic-opprettelse)
2. [Event- eller entity-consumer (record)](#2-event--eller-entity-consumer-record)
3. [Batch-consumer og @KafkaListener](#3-batch-consumer-og-kafkalistener)
4. [Request/reply: klient](#4-requestreply-klient)
5. [Request/reply: server](#5-requestreply-server)
6. [Konfigurasjon i appen](#6-konfigurasjon-i-appen)
7. [Konservativ og Balansert + kompatlag](#7-konservativ-og-balansert--kompatlag)
8. [Hvorfor dette er mulig: fint-core](#8-hvorfor-dette-er-mulig-fint-core)

---

## 1. Event-producer med topic-opprettelse

Appen injiserer Spring Boot sin `KafkaTemplate` direkte. Disse forsvinner:
- [`ParameterizedTemplateFactory`][ParameterizedTemplateFactory]
- [`ParameterizedTemplate`][ParameterizedTemplate]
- [`ParameterizedProducerRecord`][ParameterizedProducerRecord]
- step-builderne for topic-navn ([`TopicNamePrefixParameters`][TopicNamePrefixParameters], [`EventTopicNameParameters`][EventTopicNameParameters]) og topic-config ([`EventTopicConfiguration`][EventTopicConfiguration])

**v6 (Kotlin)**

Kilde (forenklet): [`InstanceRegisteredEventProducerService.kt`](https://github.com/FINTLabs/fint-flyt-instance-service/blob/main/src/main/kotlin/no/novari/flyt/instance/kafka/InstanceRegisteredEventProducerService.kt) i fint-flyt-instance-service.

```kotlin
@Service
class InstanceRegisteredProducer(
    templateFactory: ParameterizedTemplateFactory,
    eventTopicService: EventTopicService,
) {
    private val template = templateFactory.createTemplate(InstanceDto::class.java)
    private val topic = EventTopicNameParameters.builder()
        .topicNamePrefixParameters(
            TopicNamePrefixParameters.stepBuilder()
                .orgIdApplicationDefault()
                .domainContextApplicationDefault()
                .build(),
        )
        .eventName("instance-registered")
        .build()

    init {
        eventTopicService.createOrModifyTopic(
            topic,
            EventTopicConfiguration.stepBuilder()
                .partitions(1)
                .retentionTime(Duration.ofDays(7))
                .cleanupFrequency(EventCleanupFrequency.NORMAL)
                .build(),
        )
    }

    fun publish(key: String, instance: InstanceDto) =
        template.send(
            ParameterizedProducerRecord.builder<InstanceDto>()
                .topicNameParameters(topic)
                .key(key)
                .value(instance)
                .build(),
        )
}
```

v6-klasser brukt: [`ParameterizedTemplateFactory`][ParameterizedTemplateFactory], [`EventTopicService`][EventTopicService], [`EventTopicNameParameters`][EventTopicNameParameters], [`TopicNamePrefixParameters`][TopicNamePrefixParameters], [`EventTopicConfiguration`][EventTopicConfiguration], [`EventCleanupFrequency`][EventCleanupFrequency], [`ParameterizedProducerRecord`][ParameterizedProducerRecord].

**v7 Balansert (Kotlin)**

```kotlin
@Service
class InstanceRegisteredProducer(
    private val kafka: KafkaTemplate<String, InstanceDto>,
    topics: KafkaTopics,
    topicAdmin: KafkaTopicAdmin,
) {
    private val topic = topics.event("instance-registered")

    init {
        topicAdmin.createOrModify(topic, EventTopicConfig(retention = Duration.ofDays(7)))
    }

    fun publish(key: String, instance: InstanceDto) = kafka.send(topic, key, instance)
}
```

**v7 Balansert (Java)**

```java
@Service
class InstanceRegisteredProducer {
    private final KafkaTemplate<String, InstanceDto> kafka;
    private final EventTopicName topic;

    InstanceRegisteredProducer(KafkaTemplate<String, InstanceDto> kafka, KafkaTopics topics, KafkaTopicAdmin topicAdmin) {
        this.kafka = kafka;
        this.topic = topics.event("instance-registered");
        topicAdmin.createOrModify(topic, new EventTopicConfig(Duration.ofDays(7)));
    }

    CompletableFuture<SendResult<String, InstanceDto>> publish(String key, InstanceDto instance) {
        return kafka.send(topic.getName(), key, instance);
    }
}
```

- **Defaults:** org-id og domain-context hentes fra `novari.kafka.topic.*`. De kan overstyres per kall: `topics.event("x", orgId = "afk-no")`.
- **Færre parametere:** partitions = 1 og cleanup-frekvens NORMAL er defaults og kan utelates.
- **Deklarativ topic:** topic-en kan også opprettes som en `@Bean` med `topicAdmin.newTopic(topic, EventTopicConfig(...))`.
- **Delt producer:** alle templates deler én Kafka-producer. I v6 får hver template sin egen ([`ProducerFactory.java`][ProducerFactory]).
- **Origin-header:** settes fortsatt automatisk ([`OriginHeaderProducerInterceptor`][OriginHeaderProducerInterceptor]).

---

## 2. Event- eller entity-consumer (record)

Oppsettet går fra fem builder-kjeder til én metode. Containeren returneres fortsatt fra en `@Bean`, så migreringen er mekanisk.

**v6 (Java)**

Kilde (forenklet): [`UserConsumerConfiguration.java`](https://github.com/FINTLabs/fint-kontroll-user-catalog/blob/main/src/main/java/no/fintlabs/user/UserConsumerConfiguration.java) i fint-kontroll-user-catalog.

```java
@Bean
ConcurrentMessageListenerContainer<String, FactoryUser> userConsumer(
        ParameterizedListenerContainerFactoryService factoryService,
        ErrorHandlerFactory errorHandlerFactory,
        UserService userService) {
    return factoryService.createRecordListenerContainerFactory(
            FactoryUser.class,
            record -> userService.save(record.key(), record.value()),
            ListenerConfiguration.stepBuilder()
                    .groupIdApplicationDefault()
                    .maxPollRecordsKafkaDefault()
                    .maxPollIntervalKafkaDefault()
                    .seekToBeginningOnAssignment()
                    .build(),
            errorHandlerFactory.createErrorHandler(
                    ErrorHandlerConfiguration.<FactoryUser>stepBuilder()
                            .noRetries()
                            .skipFailedRecords()
                            .build()))
        .createContainer(EntityTopicNameParameters.builder()
                .topicNamePrefixParameters(TopicNamePrefixParameters.stepBuilder()
                        .orgIdApplicationDefault()
                        .domainContextApplicationDefault()
                        .build())
                .resourceName("user")
                .build());
}
```

v6-klasser brukt: [`ParameterizedListenerContainerFactoryService`][ParameterizedListenerContainerFactoryService], [`ParameterizedListenerContainerFactory`][ParameterizedListenerContainerFactory], [`ListenerConfiguration`][ListenerConfiguration] / [`ListenerConfigurationStepBuilder`][ListenerConfigurationStepBuilder], [`ErrorHandlerFactory`][ErrorHandlerFactory], [`ErrorHandlerConfiguration`][ErrorHandlerConfiguration] / [`ErrorHandlerConfigurationStepBuilder`][ErrorHandlerConfigurationStepBuilder], [`EntityTopicNameParameters`][EntityTopicNameParameters], [`TopicNamePrefixParameters`][TopicNamePrefixParameters].

**v7 Balansert (Java)**

```java
@Bean
ConcurrentMessageListenerContainer<String, FactoryUser> userConsumer(
        KafkaListenerContainers kafka, KafkaTopics topics, UserService userService) {
    return kafka.record(
            topics.entity("user"),
            FactoryUser.class,
            ListenerOptions.seekToBeginning(),
            ErrorHandling.skip(),
            record -> userService.save(record.key(), record.value()));
}
```

**v7 Balansert (Kotlin), enkleste variant**

```kotlin
@Bean
fun userConsumer(kafka: KafkaListenerContainers, topics: KafkaTopics, users: UserService) =
    kafka.record<FactoryUser>(topics.entity("user")) { record ->
        users.save(record.key(), record.value())
    }
```

**v7 Balansert (Kotlin), med retry og custom recoverer**

```kotlin
@Bean
fun instanceReceivedConsumer(kafka: KafkaListenerContainers, topics: KafkaTopics, errors: ErrorEventPublisher) =
    kafka.record<InstanceDto>(
        topics.event("instance-received"),
        errorHandling = ErrorHandling
            .retryExponential(Duration.ofSeconds(1), maxRetries = 5)
            .withRecoverer({ record, e -> errors.publish(record, e) }, OnRecoveryFailure.SKIP_RECORD),
    ) { record -> service.handle(record.value()) }
```

| v6-valg | v7 |
| --- | --- |
| `groupIdApplicationDefault()` + `maxPoll*KafkaDefault()` + `continueFromPreviousOffsetOnAssignment()` | default, utelates |
| `seekToBeginningOnAssignment()` | `ListenerOptions.seekToBeginning()` |
| `groupIdApplicationDefaultWithSuffix("-x")` | `ListenerOptions(groupId = GroupId.Suffix("-x"))`, som gir samme group-id som v6 |
| `noRetries().skipFailedRecords()` | default (`ErrorHandling.skip()`) |
| `offsetSeekingTrigger(...)` ([`OffsetSeekingTrigger`][OffsetSeekingTrigger]) | fjernet; oppskrift med `custom(...)` + `AbstractConsumerSeekAware` kommer i oppgraderingsguiden |
| container-customizer | `ListenerOptions(concurrency = …, consumerProperties = …, customizer = …)` |
| custom recoverer med log4j `TriConsumer` ([`ErrorHandlerConfiguration`][ErrorHandlerConfiguration]) | Spring sin `ConsumerRecordRecoverer` / `ConsumerAwareRecordRecoverer` |

`ErrorHandling` er ikke generisk over verditypen. Wrapper-biblioteker trenger derfor ikke lenger en egen kopi av builderen, slik [`InstanceFlowErrorHandlerConfigurationStepBuilder.kt`](https://github.com/FINTLabs/fint-flyt-kafka/blob/main/src/main/kotlin/no/novari/flyt/kafka/instanceflow/consuming/InstanceFlowErrorHandlerConfigurationStepBuilder.kt) i fint-flyt-kafka er i dag.

---

## 3. Batch-consumer og @KafkaListener

Batch fungerer som record, bare med `batch()`. Den som vil skrive ren Spring Kafka, kan bruke `@KafkaListener` og fortsatt få topic-navn og presets fra fint-kafka.

**v6 (Kotlin)**

Kilde (forenklet): [`UserMembershipConsumer.kt`](https://github.com/FINTLabs/fint-kontroll-ms-graph-gateway/blob/main/src/main/kotlin/no/novari/msgraphgateway/kafka/membership/UserMembershipConsumer.kt) i fint-kontroll-ms-graph-gateway.

```kotlin
@Bean
fun membershipConsumer() =
    factoryService.createBatchListenerContainerFactory(
        Membership::class.java,
        { batch -> service.process(batch) },
        ListenerConfiguration.stepBuilder()
            .groupIdApplicationDefault()
            .maxPollRecords(props.maxPollRecords)
            .maxPollIntervalKafkaDefault()
            .continueFromPreviousOffsetOnAssignment()
            .build(),
        errorHandlerFactory.createErrorHandler(
            ErrorHandlerConfiguration.stepBuilder<Membership>().noRetries().skipFailedRecords().build(),
        ),
        { container -> container.concurrency = props.concurrency },
    ).createContainer(topic)
```

v6-implementasjon: [`ListenerContainerFactoryService`][ListenerContainerFactoryService], [`OffsetSeekingBatchListener`][OffsetSeekingBatchListener].

**v7 Balansert (Kotlin)**

```kotlin
@Bean
fun membershipConsumer(kafka: KafkaListenerContainers, topics: KafkaTopics) =
    kafka.batch<Membership>(
        topics.entity("membership"),
        options = ListenerOptions(maxPollRecords = props.maxPollRecords, concurrency = props.concurrency),
    ) { batch -> service.process(batch) }
```

**v7 Balansert, alternativ med @KafkaListener (Kotlin)**

```kotlin
@Bean
fun cacheListenerFactory(kafka: KafkaListenerContainers) =
    kafka.containerFactory(ListenerOptions.seekToBeginning())

@Component
class RoleListener(private val cache: RoleCache) {
    @KafkaListener(
        topics = ["#{@kafkaTopics.entity('role').name}"],
        containerFactory = "cacheListenerFactory",
    )
    fun on(role: Role, @Header(KafkaHeaders.RECEIVED_KEY) key: String) = cache.put(key, role)
}
```

**v7 Balansert, alternativ med @KafkaListener (Java)**

```java
@KafkaListener(topics = "#{@kafkaTopics.entity('role').name}", containerFactory = "cacheListenerFactory")
void on(Role role, @Header(KafkaHeaders.RECEIVED_KEY) String key) {
    cache.put(key, role);
}
```

Pattern-subscriptions på flere topics, som i fint-core-consumer, blir `topics.entityPattern(resource = Segment.anyOf("elev", "person"), orgId = Segment.any())`. Verdiene regex-quotes nå automatisk. I v6 gjøres det ikke ([`TopicNamePatternParameterPattern`][TopicNamePatternParameterPattern], [`TopicNamePatternRegexUtils.kt`][TopicNamePatternRegexUtils]).

---

## 4. Request/reply: klient

Klienten blir en `RequestClient`-bean som stopper seg selv pent ved nedstengning. Reply-consumeren er ikke lenger medlem av appens consumer group. Det løser 42-sekundersproblemet fra [PR #40](https://github.com/FINTLabs/fint-kafka/pull/40) strukturelt.

I v6 starter [`RequestTemplateFactory`][RequestTemplateFactory] en `ReplyingKafkaTemplate` manuelt og stopper den aldri.

**v6 (Kotlin)**

Kilde (forenklet): [`SourceApplicationAuthorizationRequestService.kt`](https://github.com/FINTLabs/fint-flyt-web-resource-server/blob/main/src/main/kotlin/no/novari/flyt/webresourceserver/security/client/sourceapplication/SourceApplicationAuthorizationRequestService.kt) i fint-flyt-web-resource-server.

```kotlin
@Service
class AuthorizationRequestService(
    requestTemplateFactory: RequestTemplateFactory,
    replyTopicService: ReplyTopicService,
    @Value("\${novari.kafka.application-id}") applicationId: String,
) {
    private val prefix = TopicNamePrefixParameters.stepBuilder()
        .orgIdApplicationDefault().domainContextApplicationDefault().build()
    private val requestTopic = RequestTopicNameParameters.builder()
        .topicNamePrefixParameters(prefix)
        .resourceName("authorization")
        .parameterName("source-application-id")
        .build()
    private val template: RequestTemplate<String, SourceApplicationAuthorization>

    init {
        val replyTopic = ReplyTopicNameParameters.builder()
            .topicNamePrefixParameters(prefix)
            .applicationId(applicationId)
            .resourceName("authorization")
            .build()
        replyTopicService.createOrModifyTopic(
            replyTopic, ReplyTopicConfiguration.builder().retentionTime(Duration.ofMinutes(2)).build(),
        )
        template = requestTemplateFactory.createTemplate(
            replyTopic, String::class.java, SourceApplicationAuthorization::class.java, Duration.ofSeconds(5),
            ListenerConfiguration.stepBuilder()
                .groupIdApplicationDefault()
                .maxPollRecordsKafkaDefault()
                .maxPollIntervalKafkaDefault()
                .continueFromPreviousOffsetOnAssignment()
                .build(),
        )
    }

    fun get(sourceApplicationId: String): SourceApplicationAuthorization? =
        template.requestAndReceive(
            RequestProducerRecord.builder<String>()
                .topicNameParameters(requestTopic)
                .value(sourceApplicationId)
                .build(),
        ).value()
}
```

v6-klasser brukt: [`RequestTemplateFactory`][RequestTemplateFactory], [`RequestTemplate`][RequestTemplate], [`RequestProducerRecord`][RequestProducerRecord], [`ReplyTopicService`][ReplyTopicService], [`ReplyTopicConfiguration`][ReplyTopicConfiguration], [`RequestTopicNameParameters`][RequestTopicNameParameters], [`ReplyTopicNameParameters`][ReplyTopicNameParameters].

**v7 Balansert (Kotlin)**

```kotlin
@Configuration
class AuthorizationClientConfig {
    @Bean
    fun authorizationClient(
        rr: KafkaRequestReply,
        topics: KafkaTopics,
        topicAdmin: KafkaTopicAdmin,
    ): RequestClient<String, SourceApplicationAuthorization> {
        val replyTopic = topics.reply("authorization") // application-id fra novari.kafka.application-id
        topicAdmin.createOrModify(replyTopic, ReplyTopicConfig(retention = Duration.ofMinutes(2)))
        return rr.client(
            replyTopic,
            SourceApplicationAuthorization::class.java,
            RequestClientOptions(replyTimeout = Duration.ofSeconds(5)),
        )
    }
}

@Service
class AuthorizationRequestService(
    private val client: RequestClient<String, SourceApplicationAuthorization>,
    topics: KafkaTopics,
) {
    private val requestTopic = topics.request("authorization", byParameter = "source-application-id")

    fun get(id: String) = client.request(requestTopic, id).value()

    suspend fun getSuspending(id: String) = client.requestAwait(requestTopic, id).value() // valgfritt
}
```

**v7 Balansert (Java)**

```java
@Bean
RequestClient<String, SourceApplicationAuthorization> authorizationClient(
        KafkaRequestReply rr, KafkaTopics topics, KafkaTopicAdmin topicAdmin) {
    ReplyTopicName replyTopic = topics.reply("authorization");
    topicAdmin.createOrModify(replyTopic, new ReplyTopicConfig(Duration.ofMinutes(2)));
    return rr.client(replyTopic, SourceApplicationAuthorization.class,
            new RequestClientOptions(Duration.ofSeconds(5)));
}

// bruk
RequestTopicName requestTopic = topics.request("authorization", "source-application-id");
SourceApplicationAuthorization auth = client.request(requestTopic, id).value();
CompletableFuture<ConsumerRecord<String, SourceApplicationAuthorization>> future = client.requestAsync(requestTopic, id);
```

- **Timeout:** `request()` kaster `KafkaRequestTimeoutException`. I v6 ble det en generisk `RuntimeException`.
- **Request-topic per kall:** én klient kan brukes mot flere request-topics med samme reply-type.

---

## 5. Request/reply: server

Serveren blir en handler som returnerer `Reply`. Biblioteket sender svaret, venter på at sendingen lykkes, og håndterer manglende headere. I v6 sender [`RequestListenerContainerFactory`][RequestListenerContainerFactory] svaret uten å vente på resultatet (fire-and-forget), og manglende headere gir `NullPointerException`.

**v6 (Kotlin)**

Kilde (forenklet): [`ConfigurationRequestConsumerConfiguration.kt`](https://github.com/FINTLabs/fint-flyt-configuration-service/blob/main/src/main/kotlin/no/novari/flyt/configuration/kafka/ConfigurationRequestConsumerConfiguration.kt) i fint-flyt-configuration-service.

```kotlin
@Bean
fun authorizationRequestConsumer(): ConcurrentMessageListenerContainer<String, String> {
    val topic = RequestTopicNameParameters.builder()
        .topicNamePrefixParameters(prefix)
        .resourceName("authorization")
        .parameterName("source-application-id")
        .build()
    requestTopicService.createOrModifyTopic(
        topic, RequestTopicConfiguration.builder().retentionTime(Duration.ofMinutes(5)).build(),
    )
    return requestListenerContainerFactory.createRecordConsumerFactory(
        String::class.java,
        SourceApplicationAuthorization::class.java,
        { record ->
            ReplyProducerRecord.builder<SourceApplicationAuthorization>()
                .value(service.find(record.value()))
                .build()
        },
        RequestListenerConfiguration.stepBuilder(String::class.java)
            .maxPollRecordsKafkaDefault()
            .maxPollIntervalKafkaDefault()
            .build(),
        errorHandlerFactory.createErrorHandler(
            ErrorHandlerConfiguration.stepBuilder<SourceApplicationAuthorization>().noRetries().skipFailedRecords().build(),
        ),
    ).createContainer(topic)
}
```

v6-klasser brukt: [`RequestListenerContainerFactory`][RequestListenerContainerFactory], [`RequestListenerConfiguration`][RequestListenerConfiguration], [`ReplyProducerRecord`][ReplyProducerRecord], [`RequestTopicService`][RequestTopicService], [`RequestTopicConfiguration`][RequestTopicConfiguration].

**v7 Balansert (Kotlin)**

```kotlin
@Bean
fun authorizationRequestServer(rr: KafkaRequestReply, topics: KafkaTopics, topicAdmin: KafkaTopicAdmin) =
    topics.request("authorization", byParameter = "source-application-id").let { topic ->
        topicAdmin.createOrModify(topic, RequestTopicConfig(retention = Duration.ofMinutes(5)))
        rr.server<String, SourceApplicationAuthorization>(topic) { request ->
            Reply.of(service.find(request.value()))
        }
    }
```

**v7 Balansert (Java)**

```java
@Bean
ConcurrentMessageListenerContainer<String, String> authorizationRequestServer(
        KafkaRequestReply rr, KafkaTopics topics, KafkaTopicAdmin topicAdmin) {
    RequestTopicName topic = topics.request("authorization", "source-application-id");
    topicAdmin.createOrModify(topic, new RequestTopicConfig(Duration.ofMinutes(5)));
    return rr.server(topic, String.class, request -> Reply.of(service.find(request.value())));
}
```

Som alternativ kan serveren skrives med Spring sin `@KafkaListener` + `@SendTo`. Den er kompatibel med v7-klienten.

---

## 6. Konfigurasjon i appen

I v7 trenger ikke appen lenger component scan av `no.novari`, fordi alt registreres via autokonfigurasjon. All Kafka-tuning settes med vanlige `spring.kafka.*`-nøkler.

I v6 er bare [`KafkaConfiguration`][KafkaConfiguration] og health registrert i [`AutoConfiguration.imports`][AutoConfigurationImports]. Alle `@Service`-klassene må plukkes opp av appens component scan.

**v6**

```kotlin
@SpringBootApplication(scanBasePackages = ["no.novari"])
class Application
```

```yaml
spring:
  kafka:
    consumer:
      group-id: ${fint.application-id}
novari:
  kafka:
    application-id: ${fint.application-id}
    default-replicas: 1
    topic:
      org-id: novari-no
      domain-context: flyt
fint:
  kafka:
    enable-ssl: true          # injiseres av kafkarator
    health:
      enabled: true
```

**v7**

```kotlin
@SpringBootApplication
class Application
```

```yaml
spring:
  kafka:
    consumer:
      group-id: ${fint.application-id}
    security:
      protocol: SSL           # injiseres av kafkarator (fint.kafka.enable-ssl virker fortsatt, men er deprecated)
novari:
  kafka:
    application-id: ${fint.application-id}
    topic:
      org-id: novari-no
      domain-context: flyt
      default-replicas: 1
    health:
      enabled: true           # default
```

| v6 | v7 |
| --- | --- |
| `scanBasePackages = ["no.novari"]` | fjernes (autokonfigurasjon) |
| `novari.kafka.default-replicas` ([`KafkaConfigurationProperties`][KafkaConfigurationProperties]) | `novari.kafka.topic.default-replicas` |
| `fint.kafka.health.*` ([`KafkaHealthProperties.kt`][KafkaHealthProperties]) | `novari.kafka.health.*` |
| `fint.kafka.enable-ssl` ([`KafkaConfiguration`][KafkaConfiguration]) | `spring.kafka.security.protocol=SSL` |
| `@Primary ProducerConfig` for `linger.ms` / `batch.size` | `spring.kafka.producer.properties.linger.ms` osv. |
| hardkodet `auto.offset.reset=earliest` ([`KafkaConfiguration`][KafkaConfiguration]) | default `earliest`, overstyrbar med `spring.kafka.consumer.auto-offset-reset` |
| – | `novari.kafka.consumer.include-org-id-in-group-id=true` (opt-in, gir ny group-id) |

---

## 7. Konservativ og Balansert + kompatlag

Med «Konservativ» ser appkoden nesten ut som v6. Med kompatlaget kan appene oppgradere uten kodeendringer og migrere gradvis.

| | Konservativ | Balansert + kompatlag |
| --- | --- | --- |
| Kode i appen ved oppgradering | v6-eksemplene over, med samme klasser og step-buildere, nå skrevet i Kotlin | v6-koden kompilerer uendret, med deprecation-advarsler |
| Hva må endres i appen | `scanBasePackages`, property-navn, fjernede valg (f.eks. `OffsetSeekingTrigger`), custom recoverer (`TriConsumer` → Spring sin `ConsumerAwareRecordRecoverer`) | Det samme som Konservativ, og deretter overgang til v7-API-et før v8 |
| Fordeler som følger med uansett | Delt producer, ryddig livssyklus (PR #40), SSL via Boot, health som fungerer, bugfikser | Det samme |
| Estimert innsats per app (bare fint-kafka-delen) | ca. 0,5–1 t | ca. 0,5–1 t nå, deretter ca. 2–4 t gradvis |

Til sammenligning anslås «Balansert» til ca. 2–4 t per app, og koden blir som i v7-eksemplene i seksjon 1–6.

---

## 8. Hvorfor dette er mulig: fint-core

[fint-core](https://github.com/FINTLabs/fint-core) (Spring Boot 4.1.1) gikk bort fra fint-kafka og over til ren Spring Kafka. Da sto teamet igjen med å skrive bare det som er Novari-spesifikt, rundt 200 linjer. Det inkluderer:
- topic-navn: [`KafkaTopicNames.kt`](https://github.com/FINTLabs/fint-core/blob/main/fint-core-shared/src/main/kotlin/no/novari/core/shared/kafka/KafkaTopicNames.kt)
- origin-header: [`EventPublisher.kt`](https://github.com/FINTLabs/fint-core/blob/main/fint-core-adapter-gateway/src/main/java/no/fintlabs/adapter/gateway/kafka/EventPublisher.kt)
- topic-opprettelse via `KafkaAdmin`

Det er nettopp den delen v7 «Balansert» beholder. Resten av dagens ~4 300 linjer i fint-kafka er i hovedsak wrappere rundt funksjonalitet Spring Kafka allerede leverer.

<!-- Lenker til fint-kafka v6 (commit 3b6bff5) -->
[KafkaConfiguration]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/KafkaConfiguration.java
[KafkaConfigurationProperties]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/KafkaConfigurationProperties.java
[OriginHeaderProducerInterceptor]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/OriginHeaderProducerInterceptor.java
[AutoConfigurationImports]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
[KafkaHealthProperties]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/kotlin/no/novari/kafka/health/KafkaHealthProperties.kt
[ParameterizedTemplateFactory]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/producing/ParameterizedTemplateFactory.java
[ParameterizedTemplate]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/producing/ParameterizedTemplate.java
[ParameterizedProducerRecord]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/producing/ParameterizedProducerRecord.java
[ProducerFactory]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/producing/ProducerFactory.java
[ParameterizedListenerContainerFactoryService]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/ParameterizedListenerContainerFactoryService.java
[ParameterizedListenerContainerFactory]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/ParameterizedListenerContainerFactory.java
[ListenerContainerFactoryService]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/ListenerContainerFactoryService.java
[ListenerConfiguration]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/ListenerConfiguration.java
[ListenerConfigurationStepBuilder]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/ListenerConfigurationStepBuilder.java
[ErrorHandlerFactory]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/ErrorHandlerFactory.java
[ErrorHandlerConfiguration]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/ErrorHandlerConfiguration.java
[ErrorHandlerConfigurationStepBuilder]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/ErrorHandlerConfigurationStepBuilder.java
[OffsetSeekingTrigger]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/OffsetSeekingTrigger.java
[OffsetSeekingBatchListener]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/OffsetSeekingBatchListener.java
[EventTopicService]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/topic/EventTopicService.java
[EventTopicConfiguration]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/topic/configuration/EventTopicConfiguration.java
[EventCleanupFrequency]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/topic/configuration/EventCleanupFrequency.java
[EventTopicNameParameters]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/topic/name/EventTopicNameParameters.java
[EntityTopicNameParameters]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/topic/name/EntityTopicNameParameters.java
[TopicNamePrefixParameters]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/topic/name/TopicNamePrefixParameters.java
[TopicNamePatternParameterPattern]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/topic/name/TopicNamePatternParameterPattern.java
[TopicNamePatternRegexUtils]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/kotlin/no/novari/kafka/topic/name/TopicNamePatternRegexUtils.kt
[RequestTemplateFactory]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/RequestTemplateFactory.java
[RequestTemplate]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/RequestTemplate.java
[RequestProducerRecord]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/RequestProducerRecord.java
[ReplyProducerRecord]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/ReplyProducerRecord.java
[RequestListenerContainerFactory]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/RequestListenerContainerFactory.java
[RequestListenerConfiguration]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/RequestListenerConfiguration.java
[RequestTopicService]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/topic/RequestTopicService.java
[ReplyTopicService]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/topic/ReplyTopicService.java
[RequestTopicConfiguration]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/topic/configuration/RequestTopicConfiguration.java
[ReplyTopicConfiguration]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/topic/configuration/ReplyTopicConfiguration.java
[RequestTopicNameParameters]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/topic/name/RequestTopicNameParameters.java
[ReplyTopicNameParameters]: https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/topic/name/ReplyTopicNameParameters.java
