package no.fintlabs.kafka.producing.interceptors;

import no.fintlabs.kafka.consuming.ErrorHandlerConfiguration;
import no.fintlabs.kafka.consuming.ListenerConfiguration;
import no.fintlabs.kafka.consuming.ListenerContainerFactoryService;
import no.fintlabs.kafka.interceptors.OriginHeaderProducerInterceptor;
import no.fintlabs.kafka.producing.TemplateFactory;
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

    @Autowired
    TemplateFactory templateFactory;

    @Autowired
    ListenerContainerFactoryService listenerContainerFactoryService;

    @Test
    void whenKafkaTemplateSendShouldAddOriginApplicationIdHeaderToProducerRecord() throws InterruptedException {
        KafkaTemplate<String, String> kafkaTemplate = templateFactory.createTemplate(String.class);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ArrayList<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
        ConcurrentMessageListenerContainer<String, String> listenerContainer =
                listenerContainerFactoryService.createRecordListenerContainerFactory(
                        consumerRecord -> {
                            consumerRecords.add(consumerRecord);
                            countDownLatch.countDown();
                        },
                        ListenerConfiguration
                                .stepBuilder(String.class)
                                .groupIdApplicationDefault()
                                .maxPollRecordsKafkaDefault()
                                .maxPollIntervalKafkaDefault()
                                .errorHandler(
                                        ErrorHandlerConfiguration
                                                .stepBuilder(String.class)
                                                .noRetries()
                                                .skipFailedRecords()
                                                .build()
                                )
                                .continueFromPreviousOffsetOnAssignment()
                                .build(),
                        container -> {
                        }
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