package no.fintlabs.kafka.entity.polling;

import no.fintlabs.kafka.entity.topic.EntityTopicMappingService;
import no.fintlabs.kafka.entity.topic.EntityTopicNameParameters;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.util.ObjectUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

// TODO eivindmorch 26/07/2024 : Rename
public class PollingEntityProducer<T, M> {

    private final String topic;
    private final KafkaTemplate<String, T> kafkaTemplate;
    private final ReadOnlyKeyValueStore<String, T> store;

    private final Function<T, M> comparingValueMapper;

    public static <T> PollingEntityProducer<T, Integer> HashingPollingEntityProducer(
            EntityTopicMappingService entityTopicMappingService,
            EntityTopicNameParameters entityTopicNameParameters,
            KafkaTemplate<String, T> kafkaTemplate,
            Class<T> valueClass
    ) {
        return new PollingEntityProducer<>(
                entityTopicMappingService,
                entityTopicNameParameters,
                valueClass,
                kafkaTemplate,
                ObjectUtils::nullSafeHashCode
        );
    }

    // TODO eivindmorch 29/07/2024 : Lage en enklere klasse som tar inn store
    public PollingEntityProducer(
            EntityTopicMappingService entityTopicMappingService,
            EntityTopicNameParameters entityTopicNameParameters,
            Class<T> valueClass,
            KafkaTemplate<String, T> kafkaTemplate,
            Function<T, M> comparingValueMapper // TODO eivindmorch 26/07/2024 : .hashCode() eller getter feks
    ) {
        topic = entityTopicMappingService.toTopicName(entityTopicNameParameters);
        this.kafkaTemplate = kafkaTemplate;
        this.comparingValueMapper = comparingValueMapper;
        // TODO eivindmorch 29/07/2024 : Compare bytes?

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        GlobalKTable<String, T> table = streamsBuilder
                .globalTable(
                        topic,
                        Materialized
                                .<String, T>as(Stores.inMemoryKeyValueStore("name")) // TODO eivindmorch 26/07/2024 : Generate unique identifiable name
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(valueClass))// TODO eivindmorch 26/07/2024 : Decryption can be done in custom serde here
                );

        Topology topology = streamsBuilder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, new StreamsConfig(new HashMap<>()));
        kafkaStreams.start(); // TODO eivindmorch 26/07/2024 : Move
        // TODO eivindmorch 26/07/2024 : !!! Close stream !!!

        store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        table.queryableStoreName(),
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    public void send(Collection<PollingEntityProducerRecord<T>> pollingEntityProducerRecords) {
        Map<String, PollingEntityProducerRecord<T>> polledEntityProducerRecordPerKey =
                pollingEntityProducerRecords.stream()
                        .collect(Collectors.toMap(
                                PollingEntityProducerRecord::getKey,
                                Function.identity()
                        ));
        store.all().forEachRemaining(
                keyValue -> {
                    String exisitingEntityKey = keyValue.key;
                    if (polledEntityProducerRecordPerKey.containsKey(exisitingEntityKey)) {
                        T existingEntity = keyValue.value;
                        PollingEntityProducerRecord<T> polledEntityProducerRecord =
                                polledEntityProducerRecordPerKey.get(exisitingEntityKey);
                        if (!this.equals(existingEntity, polledEntityProducerRecord.getValue())) {
                            send(polledEntityProducerRecord);
                        }
                        polledEntityProducerRecordPerKey.remove(exisitingEntityKey);
                    } else {
                        // TODO eivindmorch 29/07/2024 : Delete from kafka
                    }
                }
        );
        polledEntityProducerRecordPerKey.values().forEach(this::send);
    }

    private void send(PollingEntityProducerRecord<T> pollingEntityProducerRecord) {
        kafkaTemplate.send(
                new ProducerRecord<>(
                        topic,
                        null,
                        pollingEntityProducerRecord.getKey(),
                        pollingEntityProducerRecord.getValue(),
                        pollingEntityProducerRecord.getHeaders()
                )
        );
    }

    // TODO eivindmorch 29/07/2024 : Do processing before storing in global
    // TODO eivindmorch 29/07/2024 : Compare bytes instead of caching?
    private boolean equals(T entityA, T entityB) {
        return ObjectUtils.nullSafeEquals(
                comparingValueMapper.apply(entityA),
                comparingValueMapper.apply(entityB)
        );
    }

}
