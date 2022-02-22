package no.fintlabs.kafka.services.requestreply;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.kafka.topic.ReplyTopicService;
import no.fintlabs.kafka.topic.RequestTopicService;
import no.fintlabs.kafka.topic.parameters.name.RequestTopicNameParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

@Slf4j
@Service
public class FintKafkaRequestService {

    @AllArgsConstructor
    public static class Reply<V> {
        Headers headers;
        V value;
    }

    private final RequestTopicService requestTopicService;
    private final ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate;
    private final ObjectMapper objectMapper;

    public FintKafkaRequestService(
            RequestTopicService requestTopicService, ReplyTopicService replyTopicService,
            FintKafkaReplyingTemplateFactory fintKafkaReplyingTemplateFactory,
            ObjectMapper objectMapper
    ) {
        this.requestTopicService = requestTopicService;
        this.replyingKafkaTemplate = fintKafkaReplyingTemplateFactory.create("TODO"); // TODO: 21/02/2022 TODO
        this.objectMapper = objectMapper;
    }

    public <V, R> void requestWithAsyncReplyConsumer(
            RequestTopicNameParameters requestTopicNameParameters,
            V requestValue,
            Class<R> replyValueClass,
            Collection<Header> requestHeaders,
            Consumer<Reply<R>> replyConsumer,
            Consumer<Throwable> failureConsumer
    ) throws JsonProcessingException {
        ProducerRecord<String, String> producerRecord = createRequestProducerRecord(
                requestTopicNameParameters,
                requestValue,
                requestHeaders
        );
        try {
            RequestReplyFuture<String, String, String> replyFuture = request(producerRecord);
            replyFuture.addCallback(
                    result -> {
                        try {
                            if (result == null) {
                                // TODO: 23/11/2021 Handle with exception
                                return;
                            }
                            R value = objectMapper.readValue(
                                    result.value(),
                                    replyValueClass
                            );
                            log.info("Return value: " + result.value());
                            replyConsumer.accept(new Reply<>(result.headers(), value));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    },
                    failureConsumer::accept
            );

        } catch (ExecutionException | InterruptedException e) {
            log.error("Encountered error during request: " + e);
        }
    }

    public <V, R> Optional<Reply<R>> requestAndReceive(
            RequestTopicNameParameters requestTopicNameParameters,
            V requestValue,
            Class<R> replyValueClass,
            Collection<Header> requestHeaders
    ) throws JsonProcessingException {
        ProducerRecord<String, String> producerRecord = createRequestProducerRecord(
                requestTopicNameParameters,
                requestValue,
                requestHeaders
        );
        return requestAndReceive(producerRecord)
                .flatMap((ConsumerRecord<String, String> consumerRecord) -> {
                    try {
                        return Optional.of(new Reply<>(
                                consumerRecord.headers(),
                                objectMapper.readValue(consumerRecord.value(), replyValueClass)
                        ));
                    } catch (JsonProcessingException e) {
                        // TODO: 21/02/2022 Handle
                        return Optional.empty();
                    }
                });
    }

    private <V> ProducerRecord<String, String> createRequestProducerRecord(
            RequestTopicNameParameters requestTopicNameParameters,
            V requestValue,
            Collection<Header> requestHeaders
    ) throws JsonProcessingException {
        String topicName = requestTopicService.getTopic(requestTopicNameParameters).name();
        String requestValueString = this.objectMapper.writeValueAsString(requestValue);
        return new ProducerRecord<>(
                topicName,
                null,
                (String) null,
                requestValueString,
                requestHeaders
        );
    }

    private Optional<ConsumerRecord<String, String>> requestAndReceive(ProducerRecord<String, String> producerRecord) {
        try {
            RequestReplyFuture<String, String, String> replyFuture = this.request(producerRecord);
            ConsumerRecord<String, String> consumerRecord = replyFuture.get();
            log.info("Return value: " + consumerRecord.value());
            return Optional.of(consumerRecord);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Encountered error during request: " + e);
        }
        return Optional.empty();
    }

    private RequestReplyFuture<String, String, String> request(ProducerRecord<String, String> producerRecord) throws ExecutionException, InterruptedException {
        RequestReplyFuture<String, String, String> replyFuture = replyingKafkaTemplate.sendAndReceive(producerRecord);
        SendResult<String, String> sendResult = replyFuture.getSendFuture().get();
        log.info("Sent ok: " + sendResult.getRecordMetadata());
        return replyFuture;
    }

}
