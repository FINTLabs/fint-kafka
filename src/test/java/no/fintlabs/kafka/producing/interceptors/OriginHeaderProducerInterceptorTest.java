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
                listenerContainerFactoryService.createRecordKafkaListenerContainerFactory(
                        String.class,
                        consumerRecord -> {
                            consumerRecords.add(consumerRecord);
                            countDownLatch.countDown();
                        },
                        ListenerConfiguration
                                .<String>builder()
                                .groupIdApplicationDefault()
                                .maxPollRecordsKafkaDefault()
                                .errorHandling(
                                        ErrorHandlerConfiguration
                                                .<String>builder()
                                                .noRetries()
                                                .logFailedRecords()
                                                .build()
                                )
                                .continueFromPreviousOffsetOnAssignment()
                                .build(),
                        container -> {
                        }
                ).createContainer("test-topic");

        kafkaTemplate.send("test-topic", "test-message");
        listenerContainer.start();
        countDownLatch.await(10, TimeUnit.SECONDS);

        assertThat(consumerRecords).hasSize(1);
        assertThat(consumerRecords.get(0).value()).isEqualTo("test-message");
        assertThat(consumerRecords.get(0).topic()).isEqualTo("test-topic");
        assertThat(consumerRecords.get(0).headers().headers(
                OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG
        )).hasSize(1);

        assertThat(new String(
                consumerRecords.get(0).headers().lastHeader(
                        OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG
                ).value()
        )).isEqualTo("test");

    }

}