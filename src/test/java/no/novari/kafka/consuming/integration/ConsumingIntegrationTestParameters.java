package no.novari.kafka.consuming.integration;

import lombok.AllArgsConstructor;
import lombok.Getter;
import no.novari.kafka.consumertracking.events.Event;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
class ConsumingIntegrationTestParameters<CONSUMER_INPUT, CONSUMER_RECORD> {
    private final List<String> given;
    private final List<String> should;
    private final int numberOfMessages;
    private final long commitToWaitFor;
    private final int maxPollRecords;
    private final Consumer<CONSUMER_INPUT> messageProcessor;
    private final ErrorHandlerConfiguration<CONSUMER_RECORD> errorHandlerConfiguration;
    private final List<Event<String>> expectedEvents;

    public static ConsumingIntegrationTestParametersStepBuilder
            .GivenStep<ConsumerRecord<String, String>, ConsumerRecord<String, String>> recordStepBuilder() {
        return ConsumingIntegrationTestParametersStepBuilder.firstStep(consumerRecord -> Set.of(consumerRecord.key()));
    }

    public static ConsumingIntegrationTestParametersStepBuilder
            .GivenStep<List<ConsumerRecord<String, String>>, ConsumerRecord<String, String>> batchStepBuilder() {
        return ConsumingIntegrationTestParametersStepBuilder.firstStep(consumerRecords ->
                consumerRecords
                        .stream()
                        .map(ConsumerRecord::key)
                        .collect(Collectors.toSet())
        );
    }

    @Override
    public String toString() {
        return "Given " + getDisplayTextForList(given) + " should " + getDisplayTextForList(should);
    }

    private String getDisplayTextForList(List<String> stringList) {
        if (stringList.isEmpty()) {
            return "";
        }
        if (stringList.size() == 1) {
            return stringList.getFirst();
        }
        return String.join(", ", stringList.subList(0, stringList.size() - 1)) + " and " + stringList.getLast();
    }
}
