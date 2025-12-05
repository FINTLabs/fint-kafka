package no.novari.kafka.consumertracking;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.novari.kafka.consumertracking.event.Event;
import org.assertj.core.api.Assertions;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.stereotype.Service;

import static com.fasterxml.jackson.core.json.JsonWriteFeature.QUOTE_FIELD_NAMES;

@Service
public class EventFormattingService {

    public EventFormattingService() {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(Event.class));
        provider
                .findCandidateComponents("no/novari/kafka/consumertracking/event")
                .forEach(component -> {
                            try {
                                Class<?> cls = Class.forName(component.getBeanClassName());
                                Assertions.registerFormatterForType(cls, this::toPrettyJsonString);
                            } catch (ClassNotFoundException e) {
                                throw new RuntimeException(e);
                            }
                        }
                );
    }

    public String toPrettyJsonString(Object object) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        try {
            return object
                           .getClass()
                           .getSimpleName() +
                   " " +
                   objectMapper
                           .writerWithDefaultPrettyPrinter()
                           .withoutFeatures(QUOTE_FIELD_NAMES)
                           .writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
