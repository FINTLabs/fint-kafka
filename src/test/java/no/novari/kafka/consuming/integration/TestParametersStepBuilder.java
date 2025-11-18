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
class TestParametersStepBuilder<P> {

    static <P> GivenStep<P> firstStep(Function<P, Set<String>> keyExtractor) {
        return new Steps<>(keyExtractor);
    }

    interface GivenStep<P> {
        AndGivenOrShouldStep<P> given(String givenDescription);
    }

    interface AndGivenOrShouldStep<P> extends AndGivenStep<P>, ShouldStep<P> {
    }

    interface AndGivenStep<P> {
        AndGivenOrShouldStep<P> andGiven(String givenDescription);
    }

    interface ShouldStep<P> {
        AndShouldOrNumberOfMessageStep<P> should(String behaviourDescription);
    }

    interface AndShouldOrNumberOfMessageStep<P> extends AndShouldStep<P>, NumberOfMessagesStep<P> {
    }

    interface AndShouldStep<P> {
        AndShouldOrNumberOfMessageStep<P> andShould(String behaviourDescription);
    }

    interface NumberOfMessagesStep<P> {
        CommitToWaitForStep<P> numberOfMessages(int numberOfMessages);
    }

    interface CommitToWaitForStep<P> {
        MaxPollRecordsStep<P> commitToWaitFor(long offset);
    }

    interface MaxPollRecordsStep<P> {
        MessageProcessorStep<P> maxPollRecords(int maxPollRecords);
    }

    interface MessageProcessorStep<P> {
        ErrorHandlerStep<P> noMessageProcessor();

        MessageProcessorCustomExceptionOrErrorHandlerStep<P> failAtMessage(int messageToFailAt);

        MessageProcessorCustomExceptionOrErrorHandlerStep<P> failAtMessageOnce(int messageToFailAt);

        MessageProcessorCustomExceptionOrErrorHandlerStep<P> failAtMessageNTimes(
                int messageToFailAt,
                int numberOfTimesToFail
        );

        ErrorHandlerStep<P> customMessageProcessor(Consumer<P> messageProcessor);
    }

    interface MessageProcessorCustomExceptionOrErrorHandlerStep<P> extends
            MessageProcessorCustomExceptionStep<P>, ErrorHandlerStep<P> {
    }

    interface MessageProcessorCustomExceptionStep<P> {
        ErrorHandlerStep<P> customException(Supplier<RuntimeException> customException);
    }

    interface ErrorHandlerStep<P> {
        ExpectedEventsStep<P> errorHandlerConfiguration(ErrorHandlerConfiguration<String> errorHandlerConfiguration);
    }

    interface ExpectedEventsStep<P> {
        BuildStep<P> expectedEvents(List<Event<String>> expectedEvents);
    }

    interface BuildStep<P> {
        TestParameters<P> build();
    }

    @RequiredArgsConstructor
    private static class Steps<P> implements
            GivenStep<P>,
            AndGivenOrShouldStep<P>,
            AndGivenStep<P>,
            AndShouldOrNumberOfMessageStep<P>,
            ShouldStep<P>,
            AndShouldStep<P>,
            NumberOfMessagesStep<P>,
            CommitToWaitForStep<P>,
            MaxPollRecordsStep<P>,
            MessageProcessorStep<P>,
            MessageProcessorCustomExceptionOrErrorHandlerStep<P>,
            MessageProcessorCustomExceptionStep<P>,
            ErrorHandlerStep<P>,
            ExpectedEventsStep<P>,
            BuildStep<P> {

        private final Function<P, Set<String>> keyExtractor;

        private final List<String> given = new ArrayList<>();
        private final List<String> should = new ArrayList<>();
        private int numberOfMessages;
        private long commitToWaitFor;
        private int maxPollRecords;
        private Consumer<P> messageProcessor;
        private Supplier<RuntimeException> exceptionSupplier = RuntimeException::new;
        private ErrorHandlerConfiguration<String> errorHandlerConfiguration;
        private List<Event<String>> expectedEvents;

        @Override
        public AndGivenOrShouldStep<P> given(String stateDescription) {
            this.given.add(stateDescription);
            return this;
        }

        @Override
        public AndGivenOrShouldStep<P> andGiven(String stateDescription) {
            this.given.add(stateDescription);
            return this;
        }

        @Override
        public AndShouldOrNumberOfMessageStep<P> should(String behaviourDescription) {
            should.add(behaviourDescription);
            return this;
        }

        @Override
        public AndShouldOrNumberOfMessageStep<P> andShould(String behaviourDescription) {
            should.add(behaviourDescription);
            return this;
        }


        @Override
        public CommitToWaitForStep<P> numberOfMessages(int numberOfMessages) {
            this.numberOfMessages = numberOfMessages;
            return this;
        }

        @Override
        public MaxPollRecordsStep<P> commitToWaitFor(long offset) {
            commitToWaitFor = offset;
            return this;
        }

        @Override
        public MessageProcessorStep<P> maxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        @Override
        public ErrorHandlerStep<P> noMessageProcessor() {
            messageProcessor = p -> {
            };
            return this;
        }

        @Override
        public MessageProcessorCustomExceptionOrErrorHandlerStep<P> failAtMessage(int messageToFailAt) {
            messageProcessor = createFailAtMessageMessageProcessor(messageToFailAt);
            return this;
        }

        @Override
        public MessageProcessorCustomExceptionOrErrorHandlerStep<P> failAtMessageOnce(int messageToFailAt) {
            messageProcessor = createFailAtMessageNTimesMessageProcessor(messageToFailAt, 1);
            return this;
        }

        @Override
        public MessageProcessorCustomExceptionOrErrorHandlerStep<P> failAtMessageNTimes(
                int messageToFailAt,
                int numberOfTimesToFail
        ) {
            messageProcessor = createFailAtMessageNTimesMessageProcessor(messageToFailAt, numberOfTimesToFail);
            return this;
        }

        @Override
        public ErrorHandlerStep<P> customMessageProcessor(Consumer<P> messageProcessor) {
            this.messageProcessor = messageProcessor;
            return this;
        }

        @Override
        public ErrorHandlerStep<P> customException(Supplier<RuntimeException> customExceptionSupplier) {
            this.exceptionSupplier = customExceptionSupplier;
            return this;
        }

        @Override
        public ExpectedEventsStep<P> errorHandlerConfiguration(ErrorHandlerConfiguration<String> errorHandlerConfiguration) {
            this.errorHandlerConfiguration = errorHandlerConfiguration;
            return this;
        }

        @Override
        public BuildStep<P> expectedEvents(List<Event<String>> expectedEvents) {
            this.expectedEvents = expectedEvents;
            return this;
        }

        @Override
        public TestParameters<P> build() {
            return new TestParameters<>(
                    given,
                    should,
                    numberOfMessages,
                    commitToWaitFor,
                    maxPollRecords,
                    messageProcessor,
                    errorHandlerConfiguration,
                    expectedEvents
            );
        }

        private Consumer<P> createFailAtMessageMessageProcessor(int messageToFailAt) {
            return p -> {
                if (keyExtractor.apply(p)
                                .contains("key" + messageToFailAt)) {
                    throw exceptionSupplier.get();
                }
            };
        }

        private Consumer<P> createFailAtMessageNTimesMessageProcessor(
                int messageToFailAt,
                int numberOfTimesToFail
        ) {
            CountDownLatch latch = new CountDownLatch(numberOfTimesToFail);
            return p -> {
                if (keyExtractor.apply(p)
                                .contains("key" + messageToFailAt) && latch.getCount() > 0) {
                    latch.countDown();
                    throw exceptionSupplier.get();
                }
            };
        }
    }
}
