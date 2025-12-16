package no.novari.kafka.consumertracking;

import lombok.extern.slf4j.Slf4j;
import no.novari.kafka.consumertracking.event.BatchDeliveryFailed;
import no.novari.kafka.consumertracking.event.BatchRecovered;
import no.novari.kafka.consumertracking.event.BatchRecoveryFailed;
import no.novari.kafka.consumertracking.event.CustomRecovererInvoked;
import no.novari.kafka.consumertracking.event.Event;
import no.novari.kafka.consumertracking.event.EventFormattingService;
import no.novari.kafka.consumertracking.event.ListenerFailedToProcessBatch;
import no.novari.kafka.consumertracking.event.ListenerFailedToProcessRecord;
import no.novari.kafka.consumertracking.event.ListenerInvokedWithBatch;
import no.novari.kafka.consumertracking.event.ListenerInvokedWithRecord;
import no.novari.kafka.consumertracking.event.ListenerSuccessfullyProcessedBatch;
import no.novari.kafka.consumertracking.event.ListenerSuccessfullyProcessedRecord;
import no.novari.kafka.consumertracking.event.OffsetCommitFailed;
import no.novari.kafka.consumertracking.event.OffsetsCommitted;
import no.novari.kafka.consumertracking.event.PartitionsAssigned;
import no.novari.kafka.consumertracking.event.PartitionsRevoked;
import no.novari.kafka.consumertracking.event.RecordDeliveryFailed;
import no.novari.kafka.consumertracking.event.RecordRecovered;
import no.novari.kafka.consumertracking.event.RecordRecoveryFailed;
import no.novari.kafka.consumertracking.event.RecordsPolled;
import no.novari.kafka.consumertracking.event.report.ReportMappingService;
import no.novari.kafka.consumertracking.observers.CallbackConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.FailedRecordProcessor;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.mockito.Mockito.spy;

@Slf4j
@Service
public class ConsumerTrackingService {

    private final ReportMappingService reportMappingService;
    private final EventFormattingService eventFormattingService;

    public ConsumerTrackingService(
            ReportMappingService reportMappingService,
            EventFormattingService eventFormattingService
    ) {
        this.reportMappingService = reportMappingService;
        this.eventFormattingService = eventFormattingService;
    }

    public <VALUE> ConsumerTrackingTools<VALUE> createConsumerTrackingTools(
            Predicate<Event<VALUE>> waitForEventPredicate
    ) {
        CountDownLatch stopEventLatch = new CountDownLatch(1);
        List<Event<VALUE>> events = new ArrayList<>();

        Consumer<Event<VALUE>> addEvent = event -> {
            if (stopEventLatch.getCount() > 0) {
                log.debug("Adding event:\n{}", eventFormattingService.toPrettyJsonString(event));
                events.add(event);
                if (waitForEventPredicate.test(event)) {
                    CallbackConsumerInterceptor.unregisterAllCallbacks();
                    stopEventLatch.countDown();
                }
            } else {
                log.debug("Ignoring event:\n{}", eventFormattingService.toPrettyJsonString(event));
            }
        };

        registerConsumerInterceptorCallbacks(addEvent);

        return new ConsumerTrackingTools<>(
                events,
                container -> registerTracking(
                        container,
                        addEvent
                ),
                consumerRecord -> events.add(
                        new CustomRecovererInvoked<>(
                                reportMappingService.toTopicPartition(consumerRecord),
                                reportMappingService.toKeyValueReport(consumerRecord)
                        )
                ),
                assignments ->
                        addEvent.accept(new PartitionsAssigned<>(
                                reportMappingService.toTopicPartitionAssignments(assignments))),
                partitions ->
                        addEvent.accept(new PartitionsRevoked<>(
                                reportMappingService.toTopicPartitionReports(partitions)
                        )),
                duration -> {
                    try {
                        return stopEventLatch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    private <VALUE> void registerTracking(
            ConcurrentMessageListenerContainer<String, VALUE> container,
            java.util.function.Consumer<Event<VALUE>> addEvent
    ) {
        ContainerProperties containerProperties = container.getContainerProperties();
        containerProperties
                .getKafkaConsumerProperties()
                .setProperty(
                        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                        CallbackConsumerInterceptor.class.getName()
                );
        containerProperties
                .setCommitCallback((offsetsAndMetadata, exception) -> {
                    if (exception != null) {
                        addEvent.accept(
                                new OffsetCommitFailed<>(
                                        reportMappingService.toExceptionReport(exception),
                                        reportMappingService.toTopicPartitionReports(offsetsAndMetadata.keySet())
                                )
                        );
                    }
                });
        container.setRecordInterceptor(createRecordInterceptor(addEvent));
        container.setBatchInterceptor(createBatchInterceptor(addEvent));
        container.setCommonErrorHandler(addCallbacksToErrorHandler(container.getCommonErrorHandler(), addEvent));
    }

    private <VALUE> CommonErrorHandler addCallbacksToErrorHandler(
            CommonErrorHandler errorHandler,
            java.util.function.Consumer<Event<VALUE>> addEvent
    ) {
        CommonErrorHandler spy = spy(
                errorHandler == null
                ? new DefaultErrorHandler()
                : errorHandler
        );
        if (spy instanceof FailedRecordProcessor) {
            ((FailedRecordProcessor) spy).setRetryListeners(
                    createRetryListener(addEvent)
            );
        }
        return spy;
    }

    private <VALUE> void registerConsumerInterceptorCallbacks(java.util.function.Consumer<Event<VALUE>> addEvent) {
        CallbackConsumerInterceptor.registerOnConsumeCallback(
                consumerRecords -> addEvent.accept(
                        new RecordsPolled<>(
                                reportMappingService.toKeyValueReportsPerTopicPartition(
                                        (ConsumerRecords<String, VALUE>) consumerRecords
                                )
                        )
                )
        );
        CallbackConsumerInterceptor.registerOnCommitCallback(
                commits -> addEvent.accept(
                        new OffsetsCommitted<>(
                                commits
                                        .entrySet()
                                        .stream()
                                        .collect(Collectors.toMap(
                                                entry ->
                                                        reportMappingService.toTopicPartitionReport(entry.getKey()),
                                                entry -> entry
                                                        .getValue()
                                                        .offset()
                                        ))
                        )
                )
        );
    }

    private <VALUE> RecordInterceptor<String, VALUE> createRecordInterceptor(
            java.util.function.Consumer<Event<VALUE>> addEvent
    ) {
        return new RecordInterceptor<>() {
            @Override
            public ConsumerRecord<String, VALUE> intercept(
                    @NonNull ConsumerRecord<String, VALUE> record,
                    @NonNull org.apache.kafka.clients.consumer.Consumer<String, VALUE> consumer
            ) {
                addEvent.accept(
                        new ListenerInvokedWithRecord<>(
                                reportMappingService.toTopicPartition(record),
                                reportMappingService.toKeyValueReport(record)
                        )
                );
                return record;
            }

            @Override
            public void success(
                    @NonNull ConsumerRecord<String, VALUE> record,
                    @NonNull org.apache.kafka.clients.consumer.Consumer<String, VALUE> consumer
            ) {
                addEvent.accept(
                        new ListenerSuccessfullyProcessedRecord<>(
                                reportMappingService.toTopicPartition(record),
                                reportMappingService.toKeyValueReport(record)
                        )
                );
            }

            @Override
            public void failure(
                    @NonNull ConsumerRecord<String, VALUE> record,
                    @NonNull Exception exception,
                    @NonNull org.apache.kafka.clients.consumer.Consumer<String, VALUE> consumer
            ) {
                addEvent.accept(
                        new ListenerFailedToProcessRecord<>(
                                reportMappingService.toExceptionReport(exception),
                                reportMappingService.toTopicPartition(record),
                                reportMappingService.toKeyValueReport(record)
                        )
                );
            }

            @Override
            public void afterRecord(
                    @NonNull ConsumerRecord<String, VALUE> record,
                    @NonNull org.apache.kafka.clients.consumer.Consumer<String, VALUE> consumer
            ) {

            }
        };
    }

    private <VALUE> BatchInterceptor<String, VALUE> createBatchInterceptor(
            java.util.function.Consumer<Event<VALUE>> addEvent
    ) {
        return new BatchInterceptor<>() {
            @Override
            public ConsumerRecords<String, VALUE> intercept(
                    @NonNull ConsumerRecords<String, VALUE> records,
                    @NonNull org.apache.kafka.clients.consumer.Consumer<String, VALUE> consumer
            ) {
                addEvent.accept(
                        new ListenerInvokedWithBatch<>(
                                reportMappingService.toKeyValueReportsPerTopicPartition(records)
                        )
                );
                return records;
            }

            @Override
            public void success(
                    @NonNull ConsumerRecords<String, VALUE> records,
                    @NonNull org.apache.kafka.clients.consumer.Consumer<String, VALUE> consumer
            ) {
                addEvent.accept(
                        new ListenerSuccessfullyProcessedBatch<>(
                                reportMappingService.toKeyValueReportsPerTopicPartition(records)
                        )
                );
            }

            @Override
            public void failure(
                    @NonNull ConsumerRecords<String, VALUE> records,
                    @NonNull Exception exception,
                    @NonNull org.apache.kafka.clients.consumer.Consumer<String, VALUE> consumer
            ) {
                addEvent.accept(
                        new ListenerFailedToProcessBatch<>(
                                reportMappingService.toExceptionReport(exception),
                                reportMappingService.toKeyValueReportsPerTopicPartition(records)
                        )
                );
            }
        };
    }

    private <VALUE> RetryListener createRetryListener(java.util.function.Consumer<Event<VALUE>> addEvent) {
        return new RetryListener() {
            @Override
            public void failedDelivery(
                    @NonNull ConsumerRecord<?, ?> record,
                    @NonNull Exception ex,
                    int deliveryAttempt
            ) {
                addEvent.accept(
                        new RecordDeliveryFailed<>(
                                reportMappingService.toExceptionReport(ex),
                                reportMappingService.toTopicPartition(record),
                                reportMappingService.toKeyValueReport((ConsumerRecord<String, VALUE>) record),
                                deliveryAttempt
                        )
                );
            }

            @Override
            public void recovered(@NonNull ConsumerRecord<?, ?> record, @NonNull Exception ex) {
                addEvent.accept(
                        new RecordRecovered<>(
                                reportMappingService.toTopicPartition(record),
                                reportMappingService.toKeyValueReport((ConsumerRecord<String, VALUE>) record)
                        )
                );
            }

            @Override
            public void recoveryFailed(
                    @NonNull ConsumerRecord<?, ?> record,
                    @NonNull Exception original,
                    @NonNull Exception failure
            ) {
                addEvent.accept(
                        new RecordRecoveryFailed<>(
                                reportMappingService.toExceptionReport(failure),
                                reportMappingService.toTopicPartition(record),
                                reportMappingService.toKeyValueReport((ConsumerRecord<String, VALUE>) record)
                        )
                );
            }

            @Override
            public void failedDelivery(
                    @NonNull ConsumerRecords<?, ?> records,
                    @NonNull Exception ex,
                    int deliveryAttempt
            ) {
                addEvent.accept(
                        new BatchDeliveryFailed<>(
                                reportMappingService.toExceptionReport(ex),
                                reportMappingService.toKeyValueReportsPerTopicPartition(
                                        (ConsumerRecords<String, VALUE>) records
                                ),
                                deliveryAttempt
                        )
                );
            }

            @Override
            public void recovered(@NonNull ConsumerRecords<?, ?> records, @NonNull Exception ex) {
                addEvent.accept(
                        new BatchRecovered<>(
                                reportMappingService.toKeyValueReportsPerTopicPartition(
                                        (ConsumerRecords<String, VALUE>) records
                                )
                        )
                );
            }

            @Override
            public void recoveryFailed(
                    @NonNull ConsumerRecords<?, ?> records,
                    @NonNull Exception original,
                    @NonNull Exception failure
            ) {
                addEvent.accept(
                        new BatchRecoveryFailed<>(
                                reportMappingService.toExceptionReport(failure),
                                reportMappingService.toKeyValueReportsPerTopicPartition(
                                        (ConsumerRecords<String, VALUE>) records
                                )
                        )
                );
            }
        };
    }


}
