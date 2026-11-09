<!-- Midlertidig dokument for review. PR-en som inneholder denne filen skal ikke merges. -->

# fint-kafka v7: research og beslutningsgrunnlag

Dette er funnene fra analysen 2026-10-21. Analysen dekket tre ting:
- fint-kafka sin egen kode
- bruken i ~80 søster-repoer under FINTLabs
- Kafka-oppsettet i Kustomize og operatorene (flaiserator og kafkarator)

Planen står i [plan.md](plan.md), og eksemplene i [for-og-etter.md](for-og-etter.md). Lenker til fint-kafka peker på commit `3b6bff5` (v6.3.0-rc-2-nivå).

## 1. Beslutninger fra intervjuet

| Tema | Beslutning |
| --- | --- |
| Java-baseline | Bygg med JDK 25-toolchain, publiser for Java 21 (`release`/`jvmTarget` 21). |
| Åpne PR-er | [#40](https://github.com/FINTLabs/fint-kafka/pull/40) (reply-consumer LeaveGroup) og [#28](https://github.com/FINTLabs/fint-kafka/pull/28) (konfigurerbar `auto.offset.reset`) merges i v6 som 6.3.x. v7 løser begge strukturelt. |
| Leveranse | Langlevd `v7`-branch med flere PR-er inn mot den, og RC-releaser. `main` forblir releasbar for v6. |
| API-strategi | Utred Balansert, Konservativ og Balansert + kompatlag. Valget tas før PR 7 i planen. Anbefalingen er Balansert. |
| Java-støtte | Kotlin-first og Java-vennlig: `@JvmOverloads`, `@JvmStatic`, «withers» og `fun interface`. Ingen egen Java-modul. Step-buildere fjernes. |
| Fjernes | testFixtures-artifact, `OffsetSeekingTrigger`, lavnivå-fabrikkene og ubrukte builder-valg (liste i plan.md). |
| SSL | kafkarator legger til `spring.kafka.security.protocol=SSL`. v7 får en deprecated bro fra `fint.kafka.enable-ssl`, og broen fjernes i v8. |
| Request/reply | Beholdes, bygget på Spring (`ReplyingKafkaTemplate`) med Spring-styrt livssyklus. |
| Health | Beholdes og fikses. Flyttes til `novari.kafka.health.*` og er på som default. |
| Group-id med org-id | Opt-in (`novari.kafka.consumer.include-org-id-in-group-id`). |
| Aiven-versjon | Kafka 3.x. KIP-848 (ny group-protokoll) er derfor ikke aktuelt. `CooperativeStickyAssignor` anbefales som opt-in. |
| Tracing | `feature/ffs-2221-trace-propagering` erstattes. v7 skrur på `spring.kafka.{template,listener}.observation-enabled` som overstyrbar default. |
| Scope | Bare fint-kafka og oppgraderingsguiden. Generell SB4-migrering av apper er utenfor. Apper og wrappere kan endres fritt, så dagens appkode skal ikke begrense designet. |

## 2. fint-kafka i dag (v6)

### 2.1 Størrelse

| Pakke | Filer | Linjer |
| --- | --- | --- |
| rot (config, properties, interceptor) | 4 | 171 |
| `consuming` | 13 | 1 278 |
| `producing` | 5 | 135 |
| `requestreply` (inkl. topic, config, name) | 13 | 705 |
| `topic` (services + configuration, Java + Kotlin) | 17 | 642 |
| `topic.name` (Java + Kotlin + exceptions) | 32 | 1 100 |
| `health` (Kotlin) | 6 | 310 |
| **main totalt** | **90** | **4 341** |
| test | – | 7 758 (de to PollAndProcess-testene alene: 3 918) |
| testFixtures | 33 | 1 181 |

Topic-navn tar ca. 1 200 linjer fordelt på mer enn 25 klasser. Funksjonen er likevel bare dette:
- en strengsammenslåing med tre defaults
- en tegnsjekk
- en regex-bygger

### 2.2 Wiring

- [`AutoConfiguration.imports`](https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports) inneholder bare `KafkaConfiguration` og `KafkaHealthAutoConfiguration`.
  - De ~25 `@Service`-klassene finnes bare hvis appen gjør component scan av `no.novari(.kafka)`.
  - [`docs/upgrading/v4-to-v6.md`](https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/docs/upgrading/v4-to-v6.md) dokumenterer `scanBasePackages`.
  - [`fint-kontroll-access-management-catalog`](https://github.com/FINTLabs/fint-kontroll-access-management-catalog) har en egen `NovariKafkaComponentConfiguration` for dette.
- Ingen beans har `@ConditionalOnMissingBean`.
- [`KafkaConfiguration`](https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/KafkaConfiguration.java) definerer `kafkaAdmin`, `adminClient`, `consumerConfig` og `producerConfig`.
  - `kafkaAdmin` kolliderer med Boot sin og vinner bare fordi `no.novari…` sorteres før `org.springframework…`.
  - Boot sin `KafkaTemplate`, `ConsumerFactory`, `ProducerFactory` og listener-fabrikk lages også, men brukes ikke av biblioteket.
- `spring.factories` registrerer `KafkaHealthGroupEnvironmentPostProcessor`.

### 2.3 Konfigurasjon og SSL

- Fra `spring.kafka.*` leses bare `bootstrap-servers`, `consumer.group-id` og `ssl.*`.
  - `buildConsumerProperties()` og `buildProducerProperties()` brukes ikke, så alle andre `spring.kafka.*`-nøkler ignoreres.
- Hardkodede verdier:
  - `auto.offset.reset=earliest`
  - `fetch.max.bytes`, `max.partition.fetch.bytes` og `max.request.size` = 3 MiB
- **SSL-bug:** `security.protocol` settes fra `spring.kafka.ssl.protocol`, som egentlig er TLS-versjonen. `Resource.getFile()` feiler for `classpath:`-ressurser.
- `fint.kafka.enable-ssl` leses med `@Value`. Prefiksene er blandet: `novari.kafka.*`, `fint.kafka.enable-ssl` og `fint.kafka.health.*`.
- `KafkaConfigurationProperties.topic` er ubrukt, fordi det duplikerer `KafkaTopicConfigurationProperties`.

### 2.4 Serialisering

- Spring Kafka sin `JsonSerializer`/`JsonDeserializer` (Jackson 2 `ObjectMapper`). Begge er deprecated i Spring Kafka 4.
- Produceren skriver `__TypeId__`-headere. Consumeren ignorerer dem (`useHeadersIfPresent=false`, fast måltype).
- Ingen `ErrorHandlingDeserializer`, så en poison pill gir unntak i `poll()`.
- log4j sin `TriConsumer` er en del av det offentlige API-et (`ErrorHandlerConfiguration.customRecoverer`).

### 2.5 Livssyklus

- Hver `createTemplate` lager en ny `DefaultKafkaProducerFactory`, altså én Kafka-producer per template. Ingen av dem lukkes.
- Containerfabrikker lages med `new`. De får derfor verken `ApplicationEventPublisher` eller `ApplicationContext`, og `ConsumerStoppedEvent` publiseres ikke for containere som ikke er beans.
- Biblioteket starter aldri containere. Appene returnerer dem fra `@Bean`, og det er det dominerende mønsteret.
- [`RequestTemplateFactory`](https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/RequestTemplateFactory.java) starter `ReplyingKafkaTemplate` manuelt og stopper den aldri. Det er årsaken til ~42 s gruppestopp ved deploy (PR #40).
- Reply-containeren bruker appens group-id, og reply-topic har 1 partisjon. Med flere replikaer får trolig bare én instans svarene sine. **Ikke verifisert.**

### 2.6 Bugs og «code smells»

- **Error-handler-defaults:** [`ErrorHandlerConfigurationStepBuilder.build()`](https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/consuming/ErrorHandlerConfigurationStepBuilder.java) setter alle felter eksplisitt, så `@Builder.Default` slår aldri inn. Resultatet er `restartRetryOnExceptionChange` og `restartRetryOnRecoveryFailure` = `false`, og `classificationType = null`.
- **Topic-navn:**
  - [`TopicNameService`](https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/topic/name/TopicNameService.java) pakker input inn før validering. Null-sjekkene kan derfor aldri slå til, og feilen blir NPE i stedet.
  - Pattern-verdier regex-escapes ikke.
  - `CONTAINING` er død kode.
- **Request/reply på serversiden:**
  - [`RequestListenerContainerFactory`](https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/java/no/novari/kafka/requestreply/RequestListenerContainerFactory.java) sender svar uten å vente på resultatet (fire-and-forget).
  - Manglende `REPLY_TOPIC`- eller `CORRELATION_ID`-header gir NPE eller `NoSuchElementException`.
  - Correlation-id logges som `byte[]`.
  - `requestWithAsyncReplyConsumer` blokkerer på send.
- **Health:** [`KafkaHealthGroupEnvironmentPostProcessor`](https://github.com/FINTLabs/fint-kafka/blob/3b6bff5089df2d7b8b23168848367a88b2af8248/src/main/kotlin/no/novari/kafka/health/KafkaHealthGroupEnvironmentPostProcessor.kt) legger `kafkaConnectivityHealthIndicator` og `kafkaConsumersHealthIndicator` inn i gruppene. Boot fjerner `HealthIndicator`-suffikset fra navnene, så gruppene inkluderer trolig ingen ting. `validate-group-membership=false` skjuler feilen. **Bør verifiseres og eventuelt fikses i v6.**
- **`OffsetSeekingTrigger`:** listener-listen er ikke trådsikker, og registrerte listenere fjernes aldri.
- **Duplisering:**
  - `RequestTopicService` og `ReplyTopicService`
  - `EventTopicService` og `ErrorEventTopicService`
  - de to prefix-step-builderne
  - de to stakkene med validering og mapping
- **`cleanup.policy`:** blir `"delete, compact"`, med mellomrom.

### 2.7 Bygg, CI og dokumentasjon

- **Bytecode:** toolchain 25 uten `release`, så v6-jaren krever Java 25 i praksis.
- **Pins og overstyringer:**
  - CVE-overstyringer på `buildscript`-classpath
  - Jackson pinnet til 2.21.5 i `extra[]` og til 2.22.2 på buildscript
  - log4j, netty og lz4 pinnet
- **Byggfiler:** `mavenLocal()` er med som repository, og `settings.gradle` er skrevet i Groovy.
- **CI:** kjører bare på `push` (ikke `pull_request`), og kjører `test` og deretter `build`, så testene går to ganger.
- **Dependabot:** ignorerer Spring-majors og har grupper for h2, flyway og postgres, som ikke brukes.
- **testFixtures:** publiseres som feature-variant, men brukes av 0 repoer.
- **Dokumentasjon:** `v4-to-v6.md` oppgir Java 21 og Boot 3.5.7, som er utdatert.

## 3. Bruk i søster-repoer

### 3.1 Avhengigheter

- **Direkte på `no.novari:kafka` 6.x (23 repoer):**
  - fint-altinn-service
  - fint-core-consumer, fint-core-provider-gateway
  - fint-flyt-kafka, fint-flyt-web-resource-server, fint-flyt-resource-server
  - 17 fint-kontroll-*-repoer
- **Transitivt via wrapper-biblioteker (~22 repoer):** alle fint-flyt-tjenester og -gateways.
- **Fortsatt på gammel `no.fintlabs:fint-kafka` 3.x/4.x:**
  - finterator
  - fint-flyt-altinn-gateway (Boot 2.7)
  - fint-kontroll-application-resource-gateway
  - fint-kontroll-azure-ad-gateway
- **Ren spring-kafka, uten fint-kafka:** [fint-core](https://github.com/FINTLabs/fint-core) (Boot 4.1.1). Se 3.5.
- **Plattform:** Boot 3.2.4–3.5.16, Java 21 eller 25. Ny kode skrives mest i Kotlin.

```
no.novari:kafka
├── flyt-kafka 7.x (Kotlin)            ← InstanceFlowTemplate / InstanceFlowListenerFactoryService / kopi av error-handler-builder
├── flyt-web-resource-server 4.x (Kotlin) ← én RequestTemplate (SourceApplicationAuthorizationRequestService)
├── flyt-resource-server (Java)        ← RequestTemplate + batch-cache-listener
├── flyt-gateway-starter 4.2 = flyt-kafka + flyt-web-resource-server  → 9 gateways
└── flyt-web-instance-gateway 2.3                                       → vigo-gateway
```

### 3.2 API-bruk

Tallene er antall filer som refererer til API-et, med main og test samlet.

| API | Filer (Java / Kotlin) | Repoer | Merknad |
| --- | --- | --- | --- |
| `ParameterizedTemplateFactory` | 27 / 39 | 24 | |
| `ParameterizedListenerContainerFactoryService` | 26 / 31 | 24 | |
| `createRecordListenerContainerFactory` | 24 / 41 | 29 | |
| `createBatchListenerContainerFactory` | 1 / 4 | 3 | |
| `createContainer(...PatternParameters)` | 0 / 4 | 2 | fint-core-consumer, provider-gateway |
| `ErrorHandlerFactory` / `ErrorHandlerConfiguration` | 16 / 50 | 33 | |
| `ListenerConfiguration` | 16 / 60 | 36 | |
| `RequestTemplateFactory` / `RequestTemplate` | 1 / 18 | 12 | bare `requestAndReceive` |
| `RequestListenerContainerFactory` | 0 / 11 | 8 | |
| `EntityTopicService` | 27 / 22 | 20 | |
| `EventTopicService` | 5 / 25 | 14 | |
| `ErrorEventTopicService` | 0 / 10 | 6 | bare flyt |
| `RequestTopicService` / `ReplyTopicService` | – | 9 / 13 | |
| `OffsetSeekingTrigger` | 4 / 1 | 3 | 1 reell bruk (kontroll-user-catalog, seek fra REST) |
| `TemplateFactory`, `ListenerContainerFactoryService`, `ConsumerFactoryService`, `ProducerFactory`, `TopicService`, `TopicNamePatternService` | 0 | 0 | ubrukt |
| testFixtures / `ConsumerTrackingService` | 0 | 0 | ubrukt |
| `fint.kafka.health.*` | 0 | 0 | ikke konfigurert noe sted |

**Valg i `ListenerConfiguration`:**

| Valg | Repoer |
| --- | --- |
| `groupIdApplicationDefault` | 34 |
| `...WithSuffix` | 5 |
| `...WithUniqueSuffix` | 2 |
| `maxPoll*KafkaDefault` | 38 |
| `maxPollRecords(n)` / `maxPollInterval` overstyrt | 3 |
| `continueFromPreviousOffsetOnAssignment` | 33 |
| `seekToBeginningOnAssignment` (cache-fyllende consumere) | 15 |
| `*AndPerformOperationOnAssignment`, `onAssignment`, `onRevocation` | 0 |

**Valg i `ErrorHandlerConfiguration`:**

| Valg | Repoer |
| --- | --- |
| `noRetries` + `skipFailedRecords` | ~30 |
| retry med backoff | 2 (flyt instance- og mapping-service) |
| custom recoverer | 2 |
| `retryOnly`, `continueRetryOnExceptionChange`, `reprocessRecordOnRecoveryFailure` | 0 |

**Topic-config:**
- `EventCleanupFrequency`: bare `NORMAL` brukes.
- `EntityCleanupFrequency`: `NORMAL`, og `FREQUENT` én gang.
- `orgIdCommon` / `domainContextCommon`: 0.

### 3.3 Typiske mønstre

- **~90 % av consumerne er samme oppsett:**
  - `@Bean` som returnerer en `ConcurrentMessageListenerContainer` (Spring starter den)
  - record-listener
  - default polling
  - `noRetries().skipFailedRecords()`
  - Flere kontroll-repoer har en egen `KafkaConsumerConfigurationDefaults`-hjelper for dette.
- **Producere:** `createTemplate` og `createOrModifyTopic` kalles i konstruktør eller `init`.
- **Workarounds for begrensninger i biblioteket:**
  - `@Primary ProducerConfig` for `linger.ms` og `batch.size` (fint-core-provider-gateway)
  - mutering av `kafkaConsumerProperties` (fint-core-consumer)
  - ignorerte `novari.kafka.producer-max-message-size`-nøkler (kontroll-role-factory)
  - rå `DefaultErrorHandler`-beans (kontroll-device-*)
  - `beanFactory.registerSingleton` (flyt-history-service)
  - `AdminClient.create(kafkaAdmin.configurationProperties)` for å liste og slette topics
- **Generisk typing:** feilaktig generisk typing på error handlere er vanlig, f.eks. `<ConfigurationDto>` på en `<Long>`-consumer. Typen sjekkes aldri.

### 3.4 Kildefiler brukt som eksempler

- [fint-flyt-instance-service: `InstanceRegisteredEventProducerService.kt`](https://github.com/FINTLabs/fint-flyt-instance-service/blob/main/src/main/kotlin/no/novari/flyt/instance/kafka/InstanceRegisteredEventProducerService.kt)
- [fint-kontroll-user-catalog: `UserConsumerConfiguration.java`](https://github.com/FINTLabs/fint-kontroll-user-catalog/blob/main/src/main/java/no/fintlabs/user/UserConsumerConfiguration.java)
- [fint-kontroll-ms-graph-gateway: `UserMembershipConsumer.kt`](https://github.com/FINTLabs/fint-kontroll-ms-graph-gateway/blob/main/src/main/kotlin/no/novari/msgraphgateway/kafka/membership/UserMembershipConsumer.kt)
- [fint-flyt-web-resource-server: `SourceApplicationAuthorizationRequestService.kt`](https://github.com/FINTLabs/fint-flyt-web-resource-server/blob/main/src/main/kotlin/no/novari/flyt/webresourceserver/security/client/sourceapplication/SourceApplicationAuthorizationRequestService.kt)
- [fint-flyt-configuration-service: `ConfigurationRequestConsumerConfiguration.kt`](https://github.com/FINTLabs/fint-flyt-configuration-service/blob/main/src/main/kotlin/no/novari/flyt/configuration/kafka/ConfigurationRequestConsumerConfiguration.kt)
- [fint-flyt-kafka: `InstanceFlowErrorHandlerConfigurationStepBuilder.kt`](https://github.com/FINTLabs/fint-flyt-kafka/blob/main/src/main/kotlin/no/novari/flyt/kafka/instanceflow/consuming/InstanceFlowErrorHandlerConfigurationStepBuilder.kt): en kopi av fint-kafka sin builder

### 3.5 fint-core: referanse for ren Spring Kafka

fint-core gikk over til ren Spring Kafka i commit `471e4e9` («update: move to spring kafka»). Etterpå sto teamet igjen med ~200 linjer Novari-spesifikk kode:

| Funksjon | Fil (linjer) |
| --- | --- |
| Topic-navn | [`KafkaTopicNames.kt`](https://github.com/FINTLabs/fint-core/blob/main/fint-core-shared/src/main/kotlin/no/novari/core/shared/kafka/KafkaTopicNames.kt) (8) |
| Topic-opprettelse | `KafkaTopicService.kt` (72) og `EventTopicEnsurer.kt` (39) |
| Origin-header | [`EventPublisher.kt`](https://github.com/FINTLabs/fint-core/blob/main/fint-core-adapter-gateway/src/main/java/no/fintlabs/adapter/gateway/kafka/EventPublisher.kt) (43) |
| Group-id-suffiks og consumer-config | `KafkaConfig.kt` (24) og `BufferKafkaConsumerConfig.kt` (20) |

Byggoppsettet i [`spring-service-conventions.gradle.kts`](https://github.com/FINTLabs/fint-core/blob/main/buildSrc/src/main/kotlin/spring-service-conventions.gradle.kts) er en god referanse for SB4:
- `spring-boot-starter-kafka` og `spring-boot-starter-kafka-test`
- Jackson 3 (`tools.jackson` 3.1.x), med `jackson-2-bom` ved siden av
- toolchain 25

## 4. Runtime-konfigurasjon: Kustomize og operatorer

### 4.1 Appkonfigurasjon

| Nøkkel | Repoer | Typisk verdi |
| --- | --- | --- |
| `novari.kafka.application-id` | 41 | `${fint.application-id}` |
| `novari.kafka.topic.domain-context` | 42 | `kontroll`, `flyt`, `fint-core`, `altinn` |
| `novari.kafka.topic.org-id` | 40 | `fintlabs-no` eller `novari-no` lokalt, overstyres per org i Kustomize |
| `novari.kafka.default-replicas` | 39 | `1`, og bare lokalt |
| `spring.kafka.consumer.group-id` | 45 | `${fint.application-id}`; noen få har org-prefiks |
| `spring.kafka.bootstrap-servers` | 38 | `localhost:9092`, og bare i lokale profiler |
| `fint.kafka.*` (gammelt prefiks) | ~10 | blandes med `novari.kafka.*` |

- Ingen apper setter `max-poll`, `session-timeout` eller `management.health.kafka`.
- **Flyt** bruker profilen `flyt-kafka` (`application-flyt-kafka.yaml`).
- **Kontroll** bruker `application-kafka.yaml`.

### 4.2 Kustomize

Det finnes 62 `kustomize`-mapper. Kafka-tilgang gis via Flais `Application` CRD (`spec.kafka.acls`), og overlay per org patcher tre ting:
- **ACL-topic:** `<org>.flyt.*`, `<org>.kontroll.*`, `<org>.altinn.*` eller `<org>.fint-core.*`
- **Topic-org-id:** env-variabelen `novari.kafka.topic.orgId` (dashed, f.eks. `afk-no`)
- **Group-id:** bare i kontroll (`<org>-<app>`)

ACL-prefikset er det samme som `<org-id>.<domain-context>`, så navnekonvensjonen i fint-kafka **må bevares eksakt**.

**Flyt bruker samme group-id (`${fint.application-id}`) i alle fylker på samme Aiven-cluster.** En rebalansering i ett fylke stopper derfor alle fylkene, fordi klientene bruker eager rebalancing. Det er én av to grunner til at PR #40-problemet ble så synlig.

**Topics opprettes av appene ved oppstart** via `*TopicService.createOrModifyTopic` (38 repoer). Appene har derfor `admin`-ACL. Operatorene oppretter bare brukere og ACL-er.

### 4.3 Hva operatorene legger inn

**flaiserator** ([repo](https://github.com/FINTLabs/flaiserator)):
- Lager `KafkaUserAndAcl` fra `spec.kafka.acls`.
- Monterer secreten `<app>-kafka-certificates` på `/credentials`, og legger inn `envFrom` fra secreten `<app>-kafka`.
- Legger inn `fint.org-id` (med punktum, f.eks. `afk.no`) fra labelen `fintlabs.no/org-id`.

**kafkarator** ([repo](https://github.com/FINTLabs/kafkarator)) legger dette i secreten `<app>-kafka`:

```
fint.kafka.enable-ssl                 = "true"
spring.kafka.bootstrap-servers        = <Aiven bootstrap>
spring.kafka.ssl.key-password         = <random>
spring.kafka.ssl.key-store-location   = file:/credentials/client.keystore.p12
spring.kafka.ssl.key-store-password   = <random>
spring.kafka.ssl.key-store-type       = PKCS12
spring.kafka.ssl.protocol             = SSL
spring.kafka.ssl.trust-store-location = file:/credentials/client.truststore.jks
spring.kafka.ssl.trust-store-password = <random>
spring.kafka.ssl.trust-store-type     = JKS
```

kafkarator setter ikke `spring.kafka.security.protocol`. Hvis fint-kafka bare bruker Boot sine properties, blir protokollen `PLAINTEXT`, og tilkoblingen mot Aiven feiler. Derfor velges løsning A: kafkarator endres, og v7 får en midlertidig bro.

### 4.4 Broker

Aiven for Apache Kafka (Kafka 3.x), med én tjeneste per miljø (`kafka-api`, `kafka-beta`). Autentisering skjer med mTLS. Redpanda i cluster-infra brukes bare som observability-buffer.

## 5. SB4, Spring Kafka 4 og Kafka 4-klienter

**Bekreftet:** Det meste er bekreftet i fint-core:
- `org.springframework.boot.kafka.autoconfigure` med `KafkaAutoConfiguration`, `KafkaProperties` og `ConcurrentKafkaListenerContainerFactoryConfigurer`
- starterne `spring-boot-starter-kafka` og `spring-boot-starter-kafka-test`
- Jackson 3 (`tools.jackson`)

**Verifiseres i PR 1:**
- **Pakkeflyttinger:**
  - pakkene for `DefaultKafka{Producer,Consumer}FactoryCustomizer`
  - health i `org.springframework.boot.health.contributor`
  - ny pakke og ny `spring.factories`-nøkkel for `EnvironmentPostProcessor`
- **Jackson 3-varianter i Spring Kafka:** eksakte navn på `JacksonJsonSerializer`, `JacksonJsonDeserializer` og `JacksonJsonMessageConverter`, og API-et for type-header.
- **Retry:** Spring Retry er erstattet av retry i kjernen av Spring Framework 7. Påvirker det `ExponentialBackOffWithMaxRetries`?
- **Testmiljøet:** `EmbeddedKafka` er bare KRaft, og Boot 4 bruker JUnit 6.
- **Kafka 4-klienter:**
  - Default `linger.ms` er endret fra 0 til 5.
  - Gamle protokollversjoner er fjernet. Det er uproblematisk mot Aiven 3.x.
  - KIP-848 krever broker 4.0, og er derfor ikke aktuelt.
- **Kotlin-versjon:** hvilken versjon Boot 4.1 administrerer. Den brukes som `apiVersion`, slik at Java-apper slipper konflikter i stdlib.

## 6. Konsekvenser for wrapper-biblioteker

Dette gjelder Balansert-varianten.

- **fint-flyt-kafka:**
  - `InstanceFlowTemplate` bygges om oppå `KafkaTemplate` og `JsonMapper`.
  - `InstanceFlowListenerFactoryService` blir noen få linjer over `record()`.
  - `InstanceFlowErrorHandlerConfigurationStepBuilder` slettes, fordi `ErrorHandling` ikke er generisk.
- **fint-flyt-web-resource-server og fint-flyt-resource-server:**
  - `RequestTemplate` → `RequestClient`
  - cache-listeneren → `batch(..., ListenerOptions.seekToBeginning())`
- **Konservativ-varianten:** wrapperne trenger bare SB4, Jackson 3, endringen fra `TriConsumer` og nye property-navn. Kopien av builderen blir værende.
