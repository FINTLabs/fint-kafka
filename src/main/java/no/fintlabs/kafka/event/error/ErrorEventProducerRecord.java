package no.fintlabs.kafka.event.error;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import no.fintlabs.kafka.event.error.topic.ErrorEventTopicNameParameters;
import org.apache.kafka.common.header.Headers;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ErrorEventProducerRecord {
    private ErrorEventTopicNameParameters topicNameParameters;
    private Headers headers;
    private ErrorCollection errorCollection;
}
