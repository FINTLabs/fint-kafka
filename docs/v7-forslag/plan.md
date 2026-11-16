<!-- Midlertidig dokument for review. PR-en som inneholder denne filen skal ikke merges. -->

> Planen ble laget med Claude Code 2026-10-21. Bakgrunnsfunnene står i [research.md](research.md), og eksemplene i [for-og-etter.md](for-og-etter.md).

# fint-kafka v7: Spring Boot 4, Kotlin og opprydding

## Kontekst

`no.novari:kafka` (v6.3.0-rc-2, SB 3.5.16, 4 341 linjer Java/Kotlin, ~25 `@Service`-klasser) brukes av ~45 repoer: 23 direkte, ~22 via `fint-flyt-kafka`, `fint-flyt-web-resource-server` og `fint-flyt-resource-server`. SB4 tvinger frem en ny major (v7). Vi bruker anledningen til å
- skrive biblioteket om til Kotlin,
- bygge det på Spring Boot sine egne Kafka-beans og -properties,
- rydde bort wrappere og lite brukte funksjoner,
- løse livssyklusproblemet fra PR #40 strukturelt i stedet for med lapp.

Scope: kun fint-kafka og oppgraderingsguiden for v6→v7. Generell SB4-migrering av apper er utenfor. Apper og wrapper-biblioteker *kan* endres, så dagens appkode begrenser ikke designet.

### Beslutninger tatt i intervjuet
| Tema | Beslutning |
|---|---|
| Java-baseline | Bygg med JDK 25-toolchain, `release`/`jvmTarget` = **21**. (v6 krever i praksis Java 25 i dag, fordi `release` ikke er satt.) |
| PR #40 / #28 | Merges i v6 og releases som 6.3.x. v7 løser begge strukturelt. |
| Leveranse | Langlevd `v7`-branch med mange PR-er inn mot den, RC-releaser fra branchen. `main` forblir releasbar for v6. |
| API-strategi | Planen utreder **Balansert**, **Konservativ** og **Balansert + kompatlag**. Valget tas før PR 7. Grunnmuren (PR 1–6) er felles. |
| Java-støtte | Kotlin-first, Java-vennlig: data-klasser med defaults, `fun interface`, `@JvmOverloads`/`@JvmStatic`, «withers». Ingen egen Java-modul. |
| Fjernes | testFixtures-artifact, `OffsetSeekingTrigger`, lavnivå-fabrikkene (`TemplateFactory`, `ListenerContainerFactoryService`, `ConsumerFactoryService`, novari-`ProducerFactory`, rå `TopicService`, `TopicNamePatternService`) og ubrukte builder-valg (se «Brytende endringer»). |
| SSL | **A:** kafkarator legger i tillegg inn `spring.kafka.security.protocol=SSL`. v7 bruker kun Boot-properties, pluss en deprecated bro `fint.kafka.enable-ssl` → `spring.kafka.security.protocol=SSL` som fjernes i v8. |
| Request/reply | Beholdes, bygget på Spring (`ReplyingKafkaTemplate`) og med Spring-styrt livssyklus. |
| Health | Beholdes og fikses, flyttes til `novari.kafka.health.*`, default på. |
| Group-id med org-id | Opt-in (`novari.kafka.consumer.include-org-id-in-group-id`), ikke default. |
| Aiven | Kafka 3.x, så KIP-848 er ikke aktuelt. Vi anbefaler `CooperativeStickyAssignor` (opt-in, dokumentert). |
| Tracing | Branchen `feature/ffs-2221-trace-propagering` erstattes. v7 setter `spring.kafka.template/listener.observation-enabled=true` som overstyrbar default. |

## Viktige funn (grunnlag for designet)

**Wiring og konfigurasjon**
1. **Bare `KafkaConfiguration` og health er autokonfigurert.** Resten krever `scanBasePackages = "no.novari(.kafka)"` i appen. Ingenting har `@ConditionalOnMissingBean`. Bønnen `kafkaAdmin` kolliderer med Boot sin og vinner bare på alfabetisk rekkefølge.
2. **Boot sine `KafkaProperties` brukes nesten ikke.** Bare bootstrap, group-id og ssl leses. Hardkodet: `auto.offset.reset=earliest` og grenser på 3 MiB. Apper har workarounds: `@Primary ProducerConfig`, mutering av `kafkaConsumerProperties`, og ignorerte `novari.kafka.producer-max-message-size`.
3. **SSL-bug:** `spring.kafka.ssl.protocol` (TLS-versjon) brukes som Kafka sin `security.protocol`. Det fungerer bare fordi kafkarator legger `SSL` i feil nøkkel.

**Livssyklus og serialisering**

4. **Én ny `DefaultKafkaProducerFactory` (= én Kafka-producer) per template.** Ingen av dem lukkes ved nedstengning.
5. **Containerfabrikker lages med `new`.** De får ingen event publisher, så `ConsumerStoppedEvent` og health ser dem bare når appen selv gjør dem til beans. `ReplyingKafkaTemplate` startes manuelt og stoppes aldri (PR #40).
6. **Reply-consumeren deler appens group-id**, og reply-topics har 1 partisjon. Med flere replikaer får trolig bare én instans svarene sine (verify).
7. **Ingen `ErrorHandlingDeserializer`.** En poison pill gir unntak i `poll()`. Jackson 2-`JsonSerializer`/`JsonDeserializer` er deprecated i Spring Kafka 4, og log4j sin `TriConsumer` er en del av det offentlige API-et.

**Feil i dagens kode**

8. **`ErrorHandlerConfigurationStepBuilder` overstyrer `@Builder.Default`.** Resultatet er `restartRetry*=false` og `classificationType=null`.
9. **Null-validering for topic-navn kan aldri slå til** (det blir NPE i stedet). Pattern-verdier regex-escapes ikke. `CONTAINING` er død kode.
10. **Health-gruppene inkluderer trolig ingen ting.** Gruppene peker på `kafkaConnectivityHealthIndicator`, men Boot fjerner `HealthIndicator`-suffikset fra navnet, og `validate-group-membership=false` skjuler feilen. **Bør verifiseres og eventuelt fikses i v6 også.**

**Bygg og bruk**

11. **Byggoppsettet:**
    - CVE-overstyringer på `buildscript`-classpath
    - to forskjellige Jackson-pins
    - `mavenLocal()`
    - CI kjører bare på push og kjører testene to ganger
    - dependabot blokkerer Spring-majors og har irrelevante grupper
12. **Bruken i appene:**
    - ~90 % av consumerne er samme oppsett: `@Bean`-container, record-listener, `noRetries().skipFailedRecords()`, default polling, og enten «fortsett» eller «seek til start».
    - Batch brukes i 3 repoer, patterns i 2, custom recoverer i 2.
    - Request/reply brukes i 12 repoer, og bare `requestAndReceive`.
    - `fint-flyt-kafka` har en full kopi av error-handler-builderen fordi den er generisk over verditypen.
    - `fint-core` har allerede forlatt fint-kafka til fordel for ren spring-kafka (SB 4.1.1). Den er god referanse for byggoppsettet.

## Erfaring fra fint-core: biblioteket kan forenkles kraftig

`fint-core` (SB 4.1.1) gikk bort fra fint-kafka til ren Spring Kafka (commit `471e4e9 update: move to spring kafka`). Da teamet tok selve rammeverket direkte fra Spring Boot (`spring-boot-starter-kafka`, `@KafkaListener`, Boot sin `KafkaTemplate` og `KafkaAdmin`), sto de igjen med å skrive bare det som er Novari-spesifikt. Det var rundt 200 linjer:

| Hva fint-core måtte lage selv | Fil (linjer) |
|---|---|
| Topic-navnekonvensjon | `fint-core-shared/.../kafka/KafkaTopicNames.kt` (8) |
| Topic-opprettelse via `KafkaAdmin`/`TopicBuilder` | `.../kafka/topic/KafkaTopicService.kt` (72), `EventTopicEnsurer.kt` (39) |
| Origin-header `origin.application.id` | `.../kafka/EventPublisher.kt` (43) |
| Group-id-suffiks og consumer-config | `.../config/KafkaConfig.kt` (24), `.../sync/BufferKafkaConsumerConfig.kt` (20) |

Det bekrefter tre ting for v7:
- **Kjerneverdien i fint-kafka er liten:** konvensjoner for topic-navn og topic-config, origin-header, presets for feilhåndtering og offsets, health og request/reply. Resten av dagens ~4 300 linjer er wrappere rundt det Spring Kafka allerede leverer.
- **Uten et felles bibliotek dupliserer hvert team konvensjonene.** Det gir risiko for at topic-navn og ACL-mønstre (`<org>.<domain>.*`) glir fra hverandre. fint-kafka bør derfor finnes, men som et *tynt konvensjonslag over Spring Kafka*, ikke som et parallelt rammeverk.
- **«Balansert» lander nærmest det fint-core endte med,** bare delt og testet. Apper som går helt over til `@KafkaListener` og `KafkaTemplate` skal fortsatt kunne bruke `KafkaTopics`, `KafkaTopicAdmin`, `ErrorHandling` og health uten resten. På sikt bør fint-core kunne gå tilbake til fint-kafka v7 og slette sin egen kopi (utenfor scope, men et godt mål for designet).

## Felles målarkitektur (gjelder alle varianter)

- **Autokonfigurasjon:**
  - `NovariKafkaAutoConfiguration` kommer `after = KafkaAutoConfiguration` (pakke `org.springframework.boot.kafka.autoconfigure`).
  - Alle beans har `@ConditionalOnMissingBean`, og det trengs ingen component scan.
  - `NovariKafkaHealthAutoConfiguration` legges til med `@ConditionalOnClass(HealthIndicator)`.
- **Boot sine beans brukes:**
  - `ProducerFactory`, `KafkaTemplate`, `KafkaAdmin` og `ConcurrentKafkaListenerContainerFactoryConfigurer`.
  - Bibliotekets egen oppførsel kommer inn via `DefaultKafkaProducerFactoryCustomizer`:
    - `StringSerializer` + `JacksonJsonSerializer` (Jackson 3, appens `JsonMapper`)
    - `NovariProducerInterceptor`, som setter origin-header og mater failure-tracker
  - Resultatet er **én delt producer** for hele appen.
- **Consumer-fabrikker:** Typede consumer-fabrikker avledes fra Boot sin config og caches per verditype: `ErrorHandlingDeserializer(JacksonJsonDeserializer(V))`. For `@KafkaListener` registreres `JacksonJsonMessageConverter` (verify navn).
- **Defaults via `EnvironmentPostProcessor`** (laveste presedens, overstyrbare, synlige i `/actuator/env`):
  - `spring.kafka.consumer.auto-offset-reset=earliest`
  - grenser på 3 MiB via `spring.kafka.{consumer,producer}.properties[...]`
  - `spring.kafka.admin.modify-topic-configs=true` og fail-fast
  - `spring.kafka.{template,listener}.observation-enabled=true`

  Dette løser PR #28 strukturelt.
- **SSL-bro:** `LegacySslEnvironmentPostProcessor`, deprecated.
- **Livssyklus:**
  - `novariContainerRegistry` er en `SmartLifecycle` som holder alle containere og request-klienter biblioteket lager, og stopper dem ved nedstengning (sender LeaveGroup).
  - Containere får alltid event publisher og application context.
  - Reply-containere bruker **manuell partisjonstilordning** (`sharedReplyTopic`). Da er de ikke medlem av noen consumer group, så det blir verken rebalansering eller 42-sekunders stopp. Fallback: unik group per instans med `latest`.
- **Health:** health indikatorer og gruppe-EPP med riktige navn (`kafkaConnectivity`, `kafkaConsumers`), registry-bevisst consumer-health og egen `AdminClient`-livssyklus.
- **Properties:**
  - `novari.kafka.application-id` (fallback `spring.application.name`)
  - `novari.kafka.topic.{org-id, domain-context, default-replicas}` (default-replicas flyttes hit)
  - `novari.kafka.consumer.include-org-id-in-group-id`
  - `novari.kafka.{json,origin-header,defaults}.enabled`
  - `novari.kafka.health.*` (fra `fint.kafka.health.*`)
- **Bevares bit for bit** (verifiseres med golden-tester):
  - topic-navn: `<org>.<domain>.<entity|event|request|reply>.<suffix…>`, `event.error.<navn>`, `request.<res>[.by.<param>]`, `reply.<appId>.<res>`
  - group-id-suffikser: limes på uten separator, og «unik» betyr UUID
- **Bugfikser:** error-handler-defaults, regex-quoting, validering ved oppstart, `cleanup.policy=compact,delete`, svarsending på serversiden med header-validering og avventet send.

## Variant 1: Balansert (anbefalt)

Biblioteket blir et tynt lag over Spring Kafka med konvensjoner. Det beholder det som gir reell verdi og fjerner wrapperne.

| Behov | v7-API (pakke `no.novari.kafka.*`) |
|---|---|
| Topic-navn | `KafkaTopics`-bean: `entity()`, `event()`, `errorEvent()`, `request()`, `reply()`, `*Pattern(Segment…)`. Returnerer en sealed `TopicName` / `TopicPattern`. Erstatter ~25 klasser og ~1 200 linjer. |
| Topic-opprettelse | `KafkaTopicAdmin.createOrModify(topic, EntityTopicConfig/EventTopicConfig/RequestTopicConfig/ReplyTopicConfig)` (imperativ), eller `newTopic(...)` som `@Bean` (deklarativ). Overloads sikrer at topic-typen matcher config-typen. |
| Producere | Boot sin `KafkaTemplate<String, V>` injiseres direkte. Kotlin-extension `send(topic, key, value)`. Ingen `ParameterizedTemplate`. |
| Consumere | `KafkaListenerContainers.record/batch/custom(topics, type, ListenerOptions, ErrorHandling, handler)` returnerer en container som er en bean. Pluss `containerFactory(...)`-presets for `@KafkaListener`. |
| Feilhåndtering | Ikke-generisk `ErrorHandling(retry = Retry.None/Fixed/Exponential/PerRecord, recovery = LogAndSkip/Custom(ConsumerAwareRecordRecoverer), notRetryable, …)` med statiske fabrikker `skip()`, `retryFixed()`, `retryExponential()`. Wrapper-biblioteker trenger ikke lenger å kopiere builderen. |
| Offset ved tilordning | `OnAssignment.SEEK_TO_BEGINNING` via en `ConsumerAwareRebalanceListener`. Erstatter `OffsetSeekingTrigger`; et seek fra REST lages med `custom(...)` + `AbstractConsumerSeekAware` (oppskrift i guiden). |
| Request/reply | `KafkaRequestReply.client(replyTopic, ReplyType)` gir en `RequestClient` (`SmartLifecycle`) med `request()` (synkron), `requestAsync()` (`CompletableFuture`) og `requestAwait()` (suspend, med `kotlinx-coroutines` som compileOnly). `server(requestTopic, ReqType) { Reply.of(...) }`. |

### Kombinasjon med annotasjonsbasert Spring Kafka (Balansert)

Mål: en app skal kunne bruke `@KafkaListener`, `@SendTo` og `KafkaTemplate` direkte og fortsatt få fint-kafka sine konvensjoner. Begge stilene skal fungere side om side i samme app. Eksempler står i [for-og-etter.md, seksjon 8](for-og-etter.md#8-kombinere-med-annotasjonsbasert-spring-kafka).

Designkrav som følger av dette:
- **Stabile bean-navn og egenskaper for SpEL:**
  - `kafkaTopics`
  - `TopicName.name`
  - `TopicPattern.regex` (streng, for `topicPattern`)

  Topic-objekter skal også kunne eksponeres som egne beans (`#{@roleTopic.name}`).
- **Preset-fabrikker:** `KafkaListenerContainers.containerFactory(options, errorHandling, batch)` lager fabrikken via Boot sin `ConcurrentKafkaListenerContainerFactoryConfigurer`. Den får dermed:
  - message-converter
  - observation
  - reply-template for `@SendTo`
  - event publisher
  - seek-til-start (`ConsumerAwareRebalanceListener`), group-id-regler og `ErrorHandling`
- **Default-fabrikken:** `ErrorHandling.toErrorHandler()` gir en `CommonErrorHandler`. Boot plukker opp en slik bean på default-fabrikken, så alle `@KafkaListener` uten egen fabrikk får samme feilhåndtering.
- **Message-converter:** `JacksonJsonMessageConverter` (Jackson 3) registreres, slik at `@KafkaListener fun on(x: Typed)` fungerer med Boot sin `String`-deserializer.
- **Gjelder all Kafka-trafikk i appen:** origin-header (interceptor på den delte produceren, også for `@SendTo`-svar), opt-in org-id i group-id (customizer på consumer-fabrikken) og tracing.
- **Health:** `kafkaConsumers` leser `KafkaListenerEndpointRegistry` i tillegg til registry-en og beans fra fint-kafka.
- **`@SendTo` og `RequestClient`:**
  - `RequestClient` setter `REPLY_TOPIC` og `CORRELATION_ID` slik Spring forventer. `@KafkaListener` + `@SendTo` er dermed en fullverdig server-side.
  - `KafkaRequestReply.server(...)` svarer på samme format, så klient og server kan blandes fritt.
  - En `KafkaListenerErrorHandler`-bean (`kafkaErrorReplyHandler`) sender feilsvar med fint-kafka sin feil-header, slik at klienten feiler med én gang i stedet for å vente på timeout.
- **Dokumenteres, ikke støttes:**
  - `@KafkaListener(id = …)` gir group-id = id (`idIsGroup`), og det kan endre offsets.
  - `@RetryableTopic` (retry-topics og DLT) følger ikke topic-konvensjonene og vurderes etter v7.0.
  - Request-klienten forblir programmatisk (`RequestClient`).
- **Konservativ:** samme hjelpere legges til, men tilbys som et tillegg til v6-API-et.
- **Verifiseres:**
  - At Boot 4.1 sin configurer setter reply-template på fabrikker biblioteket lager.
  - At `JacksonJsonMessageConverter` håndterer `String`-parametere som er JSON-serialisert (`"\"abc\""`) uten å beholde anførselstegnene.

## Variant 2: Konservativ

Samme navn, pakker og kall-former som i v6 (`ParameterizedTemplateFactory`, `ParameterizedListenerContainerFactoryService`, step-buildere, `*TopicNameParameters.builder()`, `*TopicService`, `RequestTemplateFactory`), skrevet om til Kotlin, med håndskrevne buildere i stedet for Lombok. Implementasjonen under er den felles grunnmuren, så delt producer, livssyklus, SSL, health og bugfikser følger med uansett.

Disse endringene kommer likevel:
- SB4 og Jackson 3
- fjernet component scan
- nytt property-navn
- `TriConsumer` erstattes av Spring sin `ConsumerAwareRecordRecoverer`
- de avtalte fjerningene

Kopien av error-handler-builderen i `fint-flyt-kafka` blir værende.

## Variant 3: Balansert + kompatlag

Kjernen er Balansert. I tillegg kommer en `no.novari.kafka.compat`-pakke (~700–900 linjer) med de gamle v6-klassene som tynne, `@Deprecated` adaptere over nytt API. Den fjernes i v8.

Fordeler:
- Apper kan oppgradere til v7 med samme innsats som i Konservativ.
- Apper kan migrere kall gradvis.

Ulemper:
- To API-flater som må testes og dokumenteres.
- Risiko for at «midlertidig» blir permanent.
- De avtalte fjerningene og `TriConsumer`-endringen gjelder fortsatt.

## Konsekvenser

Estimatene gjelder bare fint-kafka-delen av migreringen, ikke SB4 eller Jackson 3.

| | Balansert | Konservativ | Balansert + kompat |
|---|---|---|---|
| Migrering per typisk app | ca. 2–4 t (mekanisk, med oppskrift) | ca. 0,5–1 t | ca. 0,5–1 t nå, resten gradvis |
| Totalt (~45 repoer) | ca. 100–180 t | ca. 25–45 t | ca. 25–45 t + gradvis |
| Bibliotek, main LOC (nå 4 341) | ca. 1 600–2 200 | ca. 3 000–3 600 | ca. 2 400–3 000 |
| Lesbarhet og vedlikehold | Høy, nær ren Spring | Middels | Høy kjerne, men midlertidig dobbel flate |
| Wrapper-biblioteker (`fint-flyt-kafka`) | Krymper, kopien slettes | Uendret struktur | Som Balansert over tid |
| Java-ergonomi | God | Som i dag | Begge |
| Kotlin-ergonomi | Svært god (navngitte argumenter, reified, trailing lambda) | Java-aktige kjeder | Begge |
| Risiko i appene | Flere endrede linjer (group-id-likhet sikres av golden-tester) | Lav | Lav |
| Retning mot ren Spring Kafka (som `fint-core`) | Naturlig; fint-core kan senere ta v7 i bruk | Divergerer | Naturlig |
| Kan funksjonene brukes hver for seg (bare topic-navn, bare health…)? | Ja | Nei, servicene henger sammen | Ja (kjernen) |

Anbefaling: **Balansert**. fint-core viser at det som trengs på toppen av Spring Kafka er lite, og Balansert er varianten som faktisk forenkler biblioteket ned til det. Hver app får uansett en egen SB4/Jackson 3-PR, så det koster lite ekstra å konvertere kallene samtidig. Velg kompatlaget bare hvis migreringskapasiteten er flaskehalsen.

## PR-rekkefølge

Forarbeid på `main`:
- merge #40 og #28 og release 6.3.x
- vurder om health-gruppenavnene (funn 10) skal fikses i 6.3.x
- opprett branchen `v7` fra `main`

S = felles for alle varianter, V = varierer med variant.

| # | Tittel | Innhold | Test | S/V |
|---|---|---|---|---|
| 1 | build: Spring Boot 4.1 + Gradle-opprydding | Boot 4.1.x, `spring-boot-starter-kafka`, Jackson 3, versjonskatalog, `settings.gradle.kts`, toolchain 25 / release 21, Kotlin `apiVersion` tilpasset Boot sin. Fjerner buildscript-pins, `mavenLocal`, `java-test-fixtures` og `kotlin-reflect`. Bare de kodeendringene som trengs for å kompilere (SB4-pakker, `JsonMapper`, Jackson 3-serializere). testFixtures flyttes til `src/test`. | Hele dagens testsuite grønn på SB4 | S |
| 2 | ci: PR-bygg, dependabot, RC-publisering | CI på `pull_request`, én `build`. Dependabot: egen oppføring med `target-branch: v7`, slipper til Spring-majors, irrelevante grupper fjernet. Publiserer `v7.0.0-rc-N`. | – | S |
| 3 | test: skille regresjonstestene fra API-et | Porter `ConsumerTracking` til `src/test`. En adapter skiller de 32 PollAndProcess-casene fra wiringen. Nye tester: poison pill, golden test for group-id, test for LeaveGroup-tid ved nedstengning (`@Disabled` til PR 5). | Samme case grønne | S |
| 4 | feat!: autokonfig på Boot sine Kafka-beans + properties | Det som er beskrevet under «Felles målarkitektur»: autoconfig, customizers, delt producer, typede consumer-fabrikker, message-converter for `@KafkaListener`, defaults-EPP, SSL-bro, `NovariKafkaProperties`. Fjerner `kafkaAdmin`/`AdminClient`/`ConsumerConfig`/`ProducerConfig`-beans. Gammelt API virker fortsatt oppå. | `ApplicationContextRunner` (betingelser, back-off, binding, overstyring, SSL-bro med og uten eksplisitt protocol, én producer, origin-header) + regresjon | S |
| 5 | fix!: livssyklus + request/reply-mekanikk | Registry, event publisher på alle containere, reply-container med manuell tilordning, `SmartLifecycle`-klient, svarsending med validering og avventet send | LeaveGroup-test aktivert, test med to klienter på samme reply-topic, test for manglende header | S |
| 6 | feat!: health-omarbeiding | `novari.kafka.health.*`, SB4-health-pakker, riktige gruppenavn, registry-bevisst health | Actuator-test av `/health/readiness` og `/health/liveness` | S |
| 7 | feat!: topic-navn og topic-admin | B: `KafkaTopics`/`TopicName`/`KafkaTopicAdmin`. K: v6-formen i Kotlin + fikser. | Golden-tabell for navn (lik v6), test av pattern-quoting, test av at config ikke endres ved andre oppstart | V |
| 8 | feat!: produsering | B: `ParameterizedTemplate*` slettes, `send(TopicName…)`-extension. K: typede views over Boot sin template. | `ProducerConsumerIntegrationTest` | V |
| 9 | feat!: konsumering + feilhåndtering | B: `KafkaListenerContainers`, `ListenerOptions`, `ErrorHandling`, presets. K: step-buildere i Kotlin med riktige defaults. | Alle 32 PollAndProcess-case skal gi **identisk event-spor**, unntatt dokumenterte bugfiks-case; i tillegg `OnOffsetAssignment` og `@KafkaListener`: preset-fabrikk, batch, `CommonErrorHandler`-bean på default-fabrikken, SpEL-topics, at group-id er lik v6, og at health ser containerne | V |
| 10 | feat!: request/reply-API | B: `KafkaRequestReply`/`RequestClient`/`Reply` + suspend-variant. K: `RequestTemplate*` i Kotlin på PR 5-mekanikken. | Port av `RequestReplyIntegrationTest`, timeout-test, `RequestClient` mot `@KafkaListener` + `@SendTo`-server (også med feilsvar via `kafkaErrorReplyHandler`) | V |
| 10b | (Variant 3) feat: compat-pakke | Deprecated v6-adaptere over nytt API | Kjør v6-stil-testene mot compat-pakken | V |
| 11 | chore: fjern Java og Lombok, lås API-et | `explicitApi()`, ABI-dump eller binary-compatibility-validator, KDoc, ktlint | ABI-sjekk | S |
| 12 | docs: README + `docs/upgrading/v6-to-v7.md` | Ny README (norsk, Kotlin- og Java-eksempler). Oppgraderingsguide basert på `_TEMPLATE.md`, med før/etter-tabell per API og egne avsnitt for wrapper-biblioteker, for kombinasjon med `@KafkaListener` og `@SendTo`, kafkarator/SSL og drift (group-id, `CooperativeStickyAssignor`). Retter utdaterte versjoner i `v4-to-v6.md` og oppdaterer `UPGRADING.md`. | Doc-snippets kompileres som tester (valgfritt) | V |
| 13 | release: 7.0.0-rc-1 + piloter | Piloter: én Kotlin-app, én Java-app, `fint-flyt-kafka`, ett request/reply-par, kontroll-user-catalog (seek-oppskrift) og fint-core-consumer (patterns). Deretter rc-N, GA, `v7` → `main`, og en `6.x`-vedlikeholdsbranch. | Staging: SSL mot Aiven, probes, tid ved redeploy | S |

PR 4 og PR 9 har høyest risiko. Begge sikres ved at regresjonssuiten kjører uendret mot ny intern implementasjon (PR 1–6) før API-et byttes (PR 7–10).

## Før/etter: typiske oppsett i apper

Se [for-og-etter.md](for-og-etter.md) for v6 vs. v7 (Kotlin og Java) for event-producer, record- og batch-consumer, request/reply og konfigurasjon, og hvordan fint-kafka kombineres med annotasjonsbasert Spring Kafka (`@KafkaListener`, `@SendTo`).

## Brytende endringer v6 → v7 (grunnlag for oppgraderingsguiden)

**Alle varianter**
1. Krever SB 4.1+, Jackson 3 og Java 21+.
2. Fjern `no.novari(.kafka)` fra `scanBasePackages`.
3. Beansene `kafkaAdmin`, `adminClient`, `consumerConfig` og `producerConfig` er fjernet. Bruk `spring.kafka.*`, Boot sin `KafkaAdmin`, eller `AdminClient.create(kafkaAdmin.configurationProperties)`.
4. Klientkonfig kommer fra `spring.kafka.*`, med overstyrbare defaults.
5. SSL: `spring.kafka.security.protocol=SSL`. `fint.kafka.enable-ssl` er deprecated.
6. `novari.kafka.default-replicas` → `novari.kafka.topic.default-replicas`. `fint.kafka.health.*` → `novari.kafka.health.*`.
7. Én delt producer for hele appen.
8. Poison pills går til error handler eller recoverer med verdi `null`.
9. Error-handler-defaults er rettet.
10. Fjernet:
    - testFixtures
    - `OffsetSeekingTrigger`
    - `onAssignment`/`onRevocation`
    - `*AndPerformOperationOnAssignment`
    - `retryOnly`, `continueRetryOnExceptionChange`, `reprocessRecordOnRecoveryFailure`
    - `orgIdCommon`/`domainContextCommon`
    - `requestWithAsyncReplyConsumer`
    - `CONTAINING`
    - lavnivå-fabrikkene
11. `TriConsumer` → `ConsumerAwareRecordRecoverer`.
12. Pattern-verdier regex-quotes.
13. Reply-consumere er ikke lenger med i appens consumer group.
14. Containere biblioteket lager er synlige for health. En container som stoppes med vilje gir DOWN, med mindre den er unntatt.

**Bare Balansert**

15. Nye pakker. `Parameterized*`, step-buildere, `*TopicNameParameters`, `*TopicService` og `RequestTemplate*` er erstattet av `KafkaTopics`, `KafkaTopicAdmin`, `KafkaTemplate`, `KafkaListenerContainers`, `ErrorHandling` og `KafkaRequestReply`. Se før/etter-seksjonen.

**Wrapper-biblioteker (Balansert)**
- **`fint-flyt-kafka`:** `InstanceFlowTemplate` bygges om oppå `KafkaTemplate` + `JsonMapper`. `InstanceFlowListenerFactoryService` blir noen få linjer over `record()`. `InstanceFlowErrorHandlerConfigurationStepBuilder` slettes.
- **`fint-flyt-(web-)resource-server`:** `RequestTemplate` → `RequestClient`, og cache-listeneren → `batch(..., seekToBeginning())`.

**Forutsetninger utenfor fint-kafka (gjøres før eller parallelt)**
- **kafkarator:** legg til `spring.kafka.security.protocol=SSL` i `<app>-kafka`-secreten. Bakoverkompatibelt for v6. Vurder å slutte å sette `spring.kafka.ssl.protocol=SSL`.
- **Aiven-ACL:** verifiser at reply-consumere med manuell tilordning og unik `group.id` ikke trenger nye group-ACL-er.

## SB4 / Spring Kafka 4 / Kafka 4-klienter: verifiseres i PR 1

- **Kafka-autokonfig:** ligger i modulen `spring-boot-kafka`, pakke `org.springframework.boot.kafka.autoconfigure` (bekreftet i fint-core). Customizer-klassene ligger trolig samme sted.
- **Health:** pakke `org.springframework.boot.health.contributor.*`.
- **`EnvironmentPostProcessor`:** ny pakke og ny `spring.factories`-nøkkel.
- **Jackson 3:** `JsonMapper` (`tools.jackson`). Ingen `ObjectMapper`-bean uten `spring-boot-jackson2`.
- **Serializer-klasser:** `JacksonJsonSerializer`, `JacksonJsonDeserializer` og `JacksonJsonMessageConverter`, med API for type-header.
- **`EmbeddedKafka`:** bare KRaft.
- **Kafka 4-klienter:** default `linger.ms` er 5.
- **Kotlin:** Boot sin administrerte Kotlin-versjon brukes som `apiVersion`, slik at Java-apper slipper stdlib-konflikter.

## Gjenstående spørsmål (avklares underveis, ikke blokkerende for PR 1–6)

1. `__TypeId__`-headere: beholde (paritet med v6) eller droppe? Sjekk om noen ikke-fint-kafka-consumere bruker dem.
2. Default reply-timeout: 5 s (som i dag) eller 30 s?
3. Format for org-id i group-id (`<group>-<org>`?), og offset-migrering ved opt-in (runbook eller hjelper).
4. Skal `client.id` settes fra `application-id`?
5. Generiske verdityper (`List<Foo>`), dvs. `JavaType`-overloads senere?
6. Endelig valg mellom variantene, senest før PR 7.

## Verifisering

- **Hver PR:** `./gradlew build` med ktlint, unit- og integrasjonstester mot `@EmbeddedKafka`.
- **Regresjon:** de 32 PollAndProcess-casene skal gi identiske event-spor gjennom hele overgangen. Avvik er bare tillatt for dokumenterte bugfikser.
- **Golden-tester:**
  - alle topic-navnformer fra v6 skal gi samme streng
  - group-id-strenger skal være like v6
  - topic-config skal ikke endres ved andre oppstart
- **Autokonfig:** `ApplicationContextRunner` for betingelser, back-off, properties og SSL-bro.
- **Livssyklus:** når konteksten lukkes, skal gruppen være tom og en ny consumer få tilordning på under 10 s, med `session.timeout.ms` satt til 45 s.
- **Request/reply:** to klientinstanser på samme reply-topic får begge sine svar.
- **Health:** actuator-test av `/health/readiness` og `/health/liveness`.
- **RC i staging:**
  - SSL mot Aiven med kafkarator-secret
  - probes
  - tid fra «rebalancing» til «joined» ved redeploy
  - piloter: Kotlin-app, Java-app, `fint-flyt-kafka`, et request/reply-par, kontroll-user-catalog og fint-core-consumer

## Kritiske filer

- `build.gradle.kts`, `settings.gradle`, `.github/workflows/*`, `.github/dependabot.yaml`
- `src/main/java/no/novari/kafka/KafkaConfiguration.java` (erstattes av `autoconfigure/*`)
- `src/main/java/no/novari/kafka/consuming/*` (særlig `ListenerContainerFactoryService`, `ErrorHandlerFactory`, `ErrorHandlerConfigurationStepBuilder`)
- `src/main/java/no/novari/kafka/requestreply/*` (`RequestTemplateFactory`, `RequestListenerContainerFactory`)
- `src/main/java/no/novari/kafka/topic/**`, `src/main/kotlin/no/novari/kafka/topic/**`
- `src/main/kotlin/no/novari/kafka/health/*`
- `src/main/resources/META-INF/spring.factories`, `.../AutoConfiguration.imports`
- `src/test/java/no/novari/kafka/consuming/integration/PollAndProcess*IntegrationTest.java`, `src/testFixtures/**`
- `README.md`, `docs/upgrading/*`, `UPGRADING.md`
- Referanse for SB4-bygg: [`fint-core/buildSrc/.../spring-service-conventions.gradle.kts`](https://github.com/FINTLabs/fint-core/blob/main/buildSrc/src/main/kotlin/spring-service-conventions.gradle.kts)
