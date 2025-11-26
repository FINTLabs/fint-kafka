package no.novari.kafka;

import java.util.Random;
import java.util.UUID;

public class TopicNameGenerator {

    private final Random random;

    public TopicNameGenerator(int seed) {
        this.random = new Random(seed);
    }

    public String generateRandomTopicName() {
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        return UUID
                .nameUUIDFromBytes(bytes)
                .toString();
    }
}
