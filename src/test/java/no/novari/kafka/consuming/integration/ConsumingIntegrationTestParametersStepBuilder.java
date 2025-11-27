package no.novari.kafka.consuming.integration;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import no.novari.kafka.consumertracking.events.Event;
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

    static <CONSUMER_INPUT, CONSUMER_RECORD> GivenStep<CONSUMER_INPUT, CONSUMER_RECORD> firstStep(
            Function<CONSUMER_INPUT, Set<String>> keyExtractor
    ) {
        return new Steps<>(keyExtractor);
    }

    interface GivenStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD> given(String givenDescription);
    }

    interface AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD> extends AndGivenStep<CONSUMER_INPUT,
            CONSUMER_RECORD>, ShouldStep<CONSUMER_INPUT, CONSUMER_RECORD> {
    }

    interface AndGivenStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD> andGiven(String givenDescription);
    }

    interface ShouldStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD> should(String behaviourDescription);
    }

    interface AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD> extends AndShouldStep<CONSUMER_INPUT,
            CONSUMER_RECORD>, NumberOfMessagesStep<CONSUMER_INPUT, CONSUMER_RECORD> {
    }

    interface AndShouldStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD> andShould(String behaviourDescription);
    }

    interface NumberOfMessagesStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        CommitToWaitForStep<CONSUMER_INPUT, CONSUMER_RECORD> numberOfMessages(int numberOfMessages);
    }

    interface CommitToWaitForStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        MaxPollRecordsStep<CONSUMER_INPUT, CONSUMER_RECORD> commitToWaitFor(long offset);
    }

    interface MaxPollRecordsStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD> maxPollRecords(int maxPollRecords);
    }

    interface MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> noMessageProcessor();

        MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> failAtMessageOnce(String messageKeyToFailAt);

        MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> failAtMessageOnce(
                String messageKeyToFailAt,
                Supplier<RuntimeException> exceptionSupplier
        );

        MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> failAtMessageNTimes(
                String messageKeyToFailAt,
                int numberOfTimesToFail
        );

        MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> failAtMessageNTimes(
                String messageKeyToFailAt,
                int numberOfTimesToFail,
                Supplier<RuntimeException> exceptionSupplier
        );
    }

    interface MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> extends
            MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD>, ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> {
    }

    interface ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        ExpectedEventsStep<CONSUMER_INPUT, CONSUMER_RECORD> errorHandlerConfiguration(
                ErrorHandlerConfiguration<CONSUMER_RECORD> errorHandlerConfiguration
        );
    }

    interface ExpectedEventsStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        BuildStep<CONSUMER_INPUT, CONSUMER_RECORD> expectedEvents(List<Event<String>> expectedEvents);
    }

    interface BuildStep<CONSUMER_INPUT, CONSUMER_RECORD> {
        ConsumingIntegrationTestParameters<CONSUMER_INPUT, CONSUMER_RECORD> build();
    }

    @RequiredArgsConstructor
    private static class Steps<CONSUMER_INPUT, CONSUMER_RECORD> implements
            GivenStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            AndGivenStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            ShouldStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            AndShouldStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            NumberOfMessagesStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            CommitToWaitForStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            MaxPollRecordsStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            ExpectedEventsStep<CONSUMER_INPUT, CONSUMER_RECORD>,
            BuildStep<CONSUMER_INPUT, CONSUMER_RECORD> {

        private final Function<CONSUMER_INPUT, Set<String>> keyExtractor;

        private final List<String> given = new ArrayList<>();
        private final List<String> should = new ArrayList<>();
        private int numberOfMessages;
        private long commitToWaitFor;
        private int maxPollRecords;
        private final List<Consumer<CONSUMER_INPUT>> messageProcessors = new ArrayList<>();
        private ErrorHandlerConfiguration<CONSUMER_RECORD> errorHandlerConfiguration;
        private List<Event<String>> expectedEvents;

        @Override
        public AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD> given(String stateDescription) {
            this.given.add(stateDescription);
            return this;
        }

        @Override
        public AndGivenOrShouldStep<CONSUMER_INPUT, CONSUMER_RECORD> andGiven(String stateDescription) {
            this.given.add(stateDescription);
            return this;
        }

        @Override
        public AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD> should(String behaviourDescription) {
            should.add(behaviourDescription);
            return this;
        }

        @Override
        public AndShouldOrNumberOfMessageStep<CONSUMER_INPUT, CONSUMER_RECORD> andShould(String behaviourDescription) {
            should.add(behaviourDescription);
            return this;
        }


        @Override
        public CommitToWaitForStep<CONSUMER_INPUT, CONSUMER_RECORD> numberOfMessages(int numberOfMessages) {
            this.numberOfMessages = numberOfMessages;
            return this;
        }

        @Override
        public MaxPollRecordsStep<CONSUMER_INPUT, CONSUMER_RECORD> commitToWaitFor(long offset) {
            commitToWaitFor = offset;
            return this;
        }

        @Override
        public MessageProcessorStep<CONSUMER_INPUT, CONSUMER_RECORD> maxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        @Override
        public ErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> noMessageProcessor() {
            messageProcessors.add(p -> {});
            return this;
        }

        @Override
        public MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> failAtMessageOnce(String messageKeyToFailAt) {
            return failAtMessageOnce(messageKeyToFailAt, RuntimeException::new);
        }

        @Override
        public MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> failAtMessageOnce(
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
        public MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> failAtMessageNTimes(
                String messageKeyToFailAt,
                int numberOfTimesToFail
        ) {
            return failAtMessageNTimes(messageKeyToFailAt, numberOfTimesToFail, RuntimeException::new);
        }

        @Override
        public MessageProcessorOrErrorHandlerStep<CONSUMER_INPUT, CONSUMER_RECORD> failAtMessageNTimes(
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
        public ExpectedEventsStep<CONSUMER_INPUT, CONSUMER_RECORD> errorHandlerConfiguration(
                ErrorHandlerConfiguration<CONSUMER_RECORD> errorHandlerConfiguration
        ) {
            this.errorHandlerConfiguration = errorHandlerConfiguration;
            return this;
        }

        @Override
        public BuildStep<CONSUMER_INPUT, CONSUMER_RECORD> expectedEvents(List<Event<String>> expectedEvents) {
            this.expectedEvents = expectedEvents;
            return this;
        }

        @Override
        public ConsumingIntegrationTestParameters<CONSUMER_INPUT, CONSUMER_RECORD> build() {
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
