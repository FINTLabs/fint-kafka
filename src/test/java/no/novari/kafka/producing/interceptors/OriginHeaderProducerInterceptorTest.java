package no.novari.kafka.producing.interceptors;

import no.novari.kafka.consuming.ErrorHandlerConfiguration;
import no.novari.kafka.consuming.ErrorHandlerFactory;
import no.novari.kafka.consuming.ListenerConfiguration;
import no.novari.kafka.consuming.ListenerContainerFactoryService;
import no.novari.kafka.interceptors.OriginHeaderProducerInterceptor;
import no.novari.kafka.producing.TemplateFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka
class OriginHeaderProducerInterceptorTest {

    TemplateFactory templateFactory;
    ListenerContainerFactoryService listenerContainerFactoryService;
    ErrorHandlerFactory errorHandlerFactory;

    public OriginHeaderProducerInterceptorTest(
            @Autowired TemplateFactory templateFactory,
            @Autowired ListenerContainerFactoryService listenerContainerFactoryService,
            @Autowired ErrorHandlerFactory errorHandlerFactory
    ) {
        this.templateFactory = templateFactory;
        this.listenerContainerFactoryService = listenerContainerFactoryService;
        this.errorHandlerFactory = errorHandlerFactory;
    }

    @Test
    void whenKafkaTemplateSendShouldAddOriginApplicationIdHeaderToProducerRecord() throws InterruptedException {
        KafkaTemplate<String, String> kafkaTemplate = templateFactory.createTemplate(String.class);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ArrayList<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createRecordListenerContainerFactory(
                        String.class,
                        consumerRecord -> {
                            consumerRecords.add(consumerRecord);
                            countDownLatch.countDown();
                        },
                        ListenerConfiguration
                                .stepBuilder()
                                .groupIdApplicationDefault()
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .continueFromPreviousOffsetOnAssignment()
                                .build(),
                        errorHandlerFactory.createErrorHandler(
                                ErrorHandlerConfiguration
                                        .<String>stepBuilder()
                                        .noRetries()
                                        .skipFailedRecords()
                                        .build()
                        )
                ).createContainer("test-topic");

        kafkaTemplate.send("test-topic", "test-message");
        listenerContainer.start();
        assertThat(countDownLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(consumerRecords).hasSize(1);
        assertThat(consumerRecords.getFirst().value()).isEqualTo("test-message");
        assertThat(consumerRecords.getFirst().topic()).isEqualTo("test-topic");
        assertThat(consumerRecords.getFirst().headers().headers(
                OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG
        )).hasSize(1);

        assertThat(new String(
                consumerRecords.getFirst().headers().lastHeader(
                        OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG
                ).value()
        )).isEqualTo("test");

    }

}