package no.novari.kafka.consuming.integration;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import no.novari.kafka.consumertracking.event.Event;
import no.novari.kafka.consumertracking.event.reports.TopicPartitionReport;
import no.novari.kafka.consuming.ErrorHandlerConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Getter
class ConsumingIntegrationTestParametersStepBuilder {

    static <CONSUMER_INPUT, CONSUMER_RECORD, VALUE> GivenStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> firstStep(
            Function<CONSUMER_INPUT, Set<String>> keyExtractor
    ) {
        return new Steps<>(keyExtractor);
    }

    interface GivenStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> given(String givenDescription);
    }

    interface AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> extends
            AndGivenStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            ShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
    }

    interface AndGivenStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> andGiven(String givenDescription);
    }

    interface ShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> should(String behaviourDescription);
    }

    interface AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> extends
            AndShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            NumberOfMessagesStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
    }

    interface AndShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> andShould(String behaviourDescription);
    }

    interface NumberOfMessagesStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        CommitToWaitForStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> numberOfMessages(int numberOfMessages);
    }

    interface CommitToWaitForStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        MaxPollRecordsStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> commitToWaitFor(long offset);
    }

    interface MaxPollRecordsStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> maxPollRecords(int maxPollRecords);
    }

    interface MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> noMessageProcessor();

        MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> failAtMessageOnce(String messageKeyToFailAt);

        MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> failAtMessageOnce(
                String messageKeyToFailAt,
                Supplier<RuntimeException> exceptionSupplier
        );

        MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> failAtMessageNTimes(
                String messageKeyToFailAt,
                int numberOfTimesToFail
        );

        MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> failAtMessageNTimes(
                String messageKeyToFailAt,
                int numberOfTimesToFail,
                Supplier<RuntimeException> exceptionSupplier
        );
    }

    interface MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> extends
            MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
    }

    interface ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        ExpectedEventsStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> errorHandlerConfiguration(
                ErrorHandlerConfiguration<CONSUMER_RECORD> errorHandlerConfiguration
        );
    }

    interface ExpectedEventsStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        BuildStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> expectedEvents(
                Function<TopicPartitionReport, List<Event<VALUE>>> expectedEvents
        );
    }

    interface BuildStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {
        ConsumingIntegrationTestParameters<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> build();
    }

    @RequiredArgsConstructor
    private static class Steps<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> implements
            GivenStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            AndGivenStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            ShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            AndShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            NumberOfMessagesStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            CommitToWaitForStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            MaxPollRecordsStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            ExpectedEventsStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE>,
            BuildStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> {

        private final Function<CONSUMER_INPUT, Set<String>> keyExtractor;

        private final List<String> given = new ArrayList<>();
        private final List<String> should = new ArrayList<>();
        private int numberOfMessages;
        private long commitToWaitFor;
        private int maxPollRecords;
        private final List<Consumer<CONSUMER_INPUT>> messageProcessors = new ArrayList<>();
        private ErrorHandlerConfiguration<CONSUMER_RECORD> errorHandlerConfiguration;
        private Function<TopicPartitionReport, List<Event<VALUE>>> expectedEvents;

        @Override
        public AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> given(String stateDescription) {
            this.given.add(stateDescription);
            return this;
        }

        @Override
        public AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> andGiven(String stateDescription) {
            this.given.add(stateDescription);
            return this;
        }

        @Override
        public AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> should(String behaviourDescription) {
            should.add(behaviourDescription);
            return this;
        }

        @Override
        public AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> andShould(String behaviourDescription) {
            should.add(behaviourDescription);
            return this;
        }


        @Override
        public CommitToWaitForStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> numberOfMessages(int numberOfMessages) {
            this.numberOfMessages = numberOfMessages;
            return this;
        }

        @Override
        public MaxPollRecordsStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> commitToWaitFor(long offset) {
            commitToWaitFor = offset;
            return this;
        }

        @Override
        public MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> maxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        @Override
        public ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> noMessageProcessor() {
            messageProcessors.add(p -> {});
            return this;
        }

        @Override
        public MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> failAtMessageOnce(String messageKeyToFailAt) {
            return failAtMessageOnce(messageKeyToFailAt, RuntimeException::new);
        }

        @Override
        public MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> failAtMessageOnce(
                String messageKeyToFailAt,
                Supplier<RuntimeException> exceptionSupplier
        ) {
            messageProcessors.add(createFailAtMessageNTimesMessageProcessor(
                    messageKeyToFailAt,
                    1,
                    exceptionSupplier
            ));
            return this;
        }

        @Override
        public MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> failAtMessageNTimes(
                String messageKeyToFailAt,
                int numberOfTimesToFail
        ) {
            return failAtMessageNTimes(messageKeyToFailAt, numberOfTimesToFail, RuntimeException::new);
        }

        @Override
        public MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> failAtMessageNTimes(
                String messageKeyToFailAt,
                int numberOfTimesToFail,
                Supplier<RuntimeException> exceptionSupplier
        ) {
            messageProcessors.add(createFailAtMessageNTimesMessageProcessor(
                    messageKeyToFailAt,
                    numberOfTimesToFail,
                    exceptionSupplier
            ));
            return this;
        }

        @Override
        public ExpectedEventsStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> errorHandlerConfiguration(
                ErrorHandlerConfiguration<CONSUMER_RECORD> errorHandlerConfiguration
        ) {
            this.errorHandlerConfiguration = errorHandlerConfiguration;
            return this;
        }

        @Override
        public BuildStep<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> expectedEvents(
                Function<TopicPartitionReport, List<Event<VALUE>>> expectedEvents
        ) {
            this.expectedEvents = expectedEvents;
            return this;
        }

        @Override
        public ConsumingIntegrationTestParameters<CONSUMER_INPUT, CONSUMER_RECORD, VALUE> build() {
            return new ConsumingIntegrationTestParameters<>(
                    given,
                    should,
                    numberOfMessages,
                    commitToWaitFor,
                    maxPollRecords,
                    p -> messageProcessors.forEach(consumer -> consumer.accept(p)),
                    errorHandlerConfiguration,
                    expectedEvents
            );
        }

        private Consumer<CONSUMER_INPUT> createFailAtMessageNTimesMessageProcessor(
                String messageKeyToFailAt,
                int numberOfTimesToFail,
                Supplier<RuntimeException> exceptionSupplier
        ) {
            CountDownLatch latch = new CountDownLatch(numberOfTimesToFail);
            return p -> {
                if (keyExtractor
                            .apply(p)
                            .contains(messageKeyToFailAt) && latch.getCount() > 0) {
                    latch.countDown();
                    throw exceptionSupplier.get();
                }
            };
        }
    }
}
