package no.fintlabs.kafka.common;

import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
public class ListenerBeanRegistrationService {

    private final GenericApplicationContext genericApplicationContext;

    public ListenerBeanRegistrationService(GenericApplicationContext genericApplicationContext) {
        this.genericApplicationContext = genericApplicationContext;
    }

    public String registerBean(ConcurrentMessageListenerContainer<?, ?> listenerContainer) {
        String hashCodeString = String.valueOf(listenerContainer.hashCode());
        genericApplicationContext.registerBean(
                hashCodeString,
                ConcurrentMessageListenerContainer.class,
                () -> listenerContainer
        );
        return hashCodeString;
    }

}
