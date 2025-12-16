package no.novari.kafka.consuming;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Builder(toBuilder = true)
@EqualsAndHashCode
@ToString
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ListenerConfiguration {

    private final String groupIdSuffix;

    private final Integer maxPollRecords;

    private final Duration maxPollInterval;

    private final BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback> onPartitionsAssigned;
    private final Consumer<Collection<TopicPartition>> onPartitionsRevoked;

    private final OffsetSeekingTrigger offsetSeekingTrigger;

    public static ListenerConfigurationStepBuilder.GroupIdSuffixStep stepBuilder() {
        return ListenerConfigurationStepBuilder.firstStep();
    }

    public Optional<String> getGroupIdSuffix() {
        return Optional.ofNullable(groupIdSuffix);
    }

    public Optional<Integer> getMaxPollRecords() {
        return Optional.ofNullable(maxPollRecords);
    }

    public Optional<Duration> getMaxPollInterval() {
        return Optional.ofNullable(maxPollInterval);
    }

    public Optional<BiConsumer<Map<TopicPartition, Long>, ConsumerSeekCallback>> getOnPartitionsAssigned() {
        return Optional.ofNullable(onPartitionsAssigned);
    }

    public Optional<Consumer<Collection<TopicPartition>>> getOnPartitionsRevoked() {
        return Optional.ofNullable(onPartitionsRevoked);
    }

    public Optional<OffsetSeekingTrigger> getOffsetSeekingTrigger() {
        return Optional.ofNullable(offsetSeekingTrigger);
    }
}
