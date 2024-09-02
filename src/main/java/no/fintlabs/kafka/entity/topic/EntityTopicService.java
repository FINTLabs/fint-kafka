package no.fintlabs.kafka.entity.topic;

import no.fintlabs.kafka.common.topic.TopicService;
import no.fintlabs.kafka.common.topic.configuration.values.CleanupFrequency;
import no.fintlabs.kafka.common.topic.configuration.values.NumberOfMessages;
import no.fintlabs.kafka.event.topic.EventTopicConfiguration;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class EntityTopicService {

    private final TopicService topicService;
    private final EntityTopicMappingService entityTopicMappingService;

    private final EntityTopicConfigurationMappingService entityTopicConfigurationMappingService;

    public EntityTopicService(
            TopicService topicService,
            EntityTopicMappingService entityTopicMappingService,
            EntityTopicConfigurationMappingService entityTopicConfigurationMappingService
    ) {
        this.topicService = topicService;
        this.entityTopicMappingService = entityTopicMappingService;
        this.entityTopicConfigurationMappingService = entityTopicConfigurationMappingService;
    }

    // TODO eivindmorch 23/07/2024 : Producer configs: compression

    // TODO eivindmorch 23/07/2024 : Lage automatisk oppsett for entity producers med polling
    //  1. Poll av helt datasett
    //      a. Hent map<key, value(hash)?> fra topic med kafka stream på oppstart
    //      b. Start polling når map er klart
    //      c. Sjekk i map om pollet entity er lik entity på topic
    //      d. Sjekk om map har verdi som ikke er i pollet dataset --> Slett fra topic (tombstone/null)
    //  2. Poll av datasett med delta-funksjon
    //      a. Publiser delta-parameter på egen kafka topic for å opprettholde tilstand på tvers av pod restart / deploy
    //  !! Dersom man har retention time (lastEntityRetentionTime) på entities MÅ man ha refresh-funksjonalitet:
    //      - Publisere timestamp for når siste refresh ble gjort på egen topic
    //      - Lage innebygget flux som gir signal om at data må refreshes
    //      - Når timestamp for siste refresh er lenger siden enn retentionTime - refreshMargin --> send refresh-signal

    private void a() {
        // TODO eivindmorch 22/07/2024 : Remove
        EntityTopicConfiguration
                .builder()
                .lastValueRetainedForever()
                .nullValueRetentionTimeDefault()
                .cleanupFrequency(CleanupFrequency.NORMAL)
                .contentDependentMaxSegmentSize(
                        NumberOfMessages.HUNDREDS_OF_THOUSANDS,
                        DataSize.ofMegabytes(1)
                );

        EntityTopicConfiguration
                .builder()
                .lastValueRetainedForever()
                .nullValueRetentionTimeDefault()
                .cleanupFrequencyDefault()
                .contentDependentMaxSegmentSize(
                        NumberOfMessages.THOUSANDS,
                        DataSize.ofKilobytes(10)
                )
                .build();

        EntityTopicService entityTopicService;

        entityTopicService.createOrModifyTopic(
                EntityTopicNameParameters.builder().build(),
                EntityTopicConfiguration
                        .builder()
                        .lastValueRetainedForever()
                        .nullValueRetentionTime(Duration.ofHours(7))
                        .cleanupFrequency(CleanupFrequency.NORMAL)
                        .contentDependentMaxSegmentSize(
                                NumberOfMessages.HUNDREDS_OF_THOUSANDS,
                                DataSize.ofKilobytes(30)
                        )
                        .build()
        );

        EntityTopicConfiguration
                .builder()
                .lastValueRetentionTime(Durati)
                .nullValueRetentionTime(Duration.ofDays(7))
                .cleanupFrequency(CleanupFrequency.FREQUENT)
                .contentDependentMaxSegmentSize(
                        NumberOfMessages.HUNDREDS,
                        DataSize.ofKilobytes(10)
                )
                .build();

        EventTopicConfiguration
                .builder()
                .retentionTime(Duration.ofDays(7))
                .cleanupFrequency(CleanupFrequency.NORMAL)
                .build();
    }

    public void createOrModifyTopic(
            EntityTopicNameParameters entityTopicNameParameters,
            EntityTopicConfiguration entityTopicConfiguration
    ) {
        topicService.createOrModifyTopic(
                entityTopicMappingService.toTopicName(entityTopicNameParameters),
                entityTopicConfigurationMappingService.toTopicConfiguration(entityTopicConfiguration)
        );
    }

    public TopicDescription getTopic(EntityTopicNameParameters topicNameParameters) {
        return topicService.getTopic(entityTopicMappingService.toTopicName(topicNameParameters));
    }

    public Map<String, String> getTopicConfig(EntityTopicNameParameters topicNameParameters)
            throws ExecutionException, InterruptedException {
        return topicService.getTopicConfig(entityTopicMappingService.toTopicName(topicNameParameters));
    }

}
