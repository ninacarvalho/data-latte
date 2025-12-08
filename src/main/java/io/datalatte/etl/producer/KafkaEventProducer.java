package io.datalatte.etl.producer;

import io.datalatte.etl.model.GithubEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link Producer} that sends {@link GithubEvent} objects to a Kafka topic.
 * This class is marked as a Spring {@link Component}, so it's automatically detected and registered in the application context.
 */
@Component
public class KafkaEventProducer implements Producer<GithubEvent> {
    /** The Kafka topic to which the events will be sent. */
    public static final String TOPIC = "etl.github.events";

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private final KafkaTemplate<String, GithubEvent> template;

    /**
     * Constructs a new KafkaEventProducer with the given KafkaTemplate.
     *
     * @param template The {@link KafkaTemplate} used for sending messages.
     */
    public KafkaEventProducer(KafkaTemplate<String, GithubEvent> template) {
        this.template = Objects.requireNonNull(template);
    }

    /**
     * Sends a single {@link GithubEvent} to the Kafka topic.
     * The message is sent asynchronously.
     *
     * @param e The {@link GithubEvent} to send.
     */
    @Override
    public void send(GithubEvent e) {
        // Create a Kafka producer record with the topic, key (event ID), and value (the event object).
        var rec = new ProducerRecord<>(TOPIC, e.getId(), e);
        // Add custom headers to the record for metadata.
        rec.headers()
                .add(new RecordHeader("eventType", bytes(e.getEventType())))
                .add(new RecordHeader("source",    bytes("github")));

        // Asynchronously send the record.
        CompletableFuture<SendResult<String, GithubEvent>> future = template.send(rec);

        // Add a callback to handle the result of the send operation.
        future.whenComplete((res, ex) -> {
            if (ex == null) {
                // The message was sent successfully.
                var md = res.getRecordMetadata();
                log.info("produced key={} topic={} partition={} offset={}",
                        e.getId(), md.topic(), md.partition(), md.offset());
            } else {
                // An error occurred.
                log.error("produce failed key={} reason={}", e.getId(), ex.toString());
            }
        });
    }

    /**
     * Sends a batch of {@link GithubEvent}s by iterating over the list and sending them one by one.
     *
     * @param batch The list of {@link GithubEvent}s to send.
     */
    @Override
    public void sendBatch(List<GithubEvent> batch) {
        for (GithubEvent e : batch) {
            send(e);
        }
    }

    /**
     * Helper method to convert a string to a byte array using UTF-8 encoding.
     *
     * @param s The string to convert.
     * @return The byte array representation of the string.
     */
    private static byte[] bytes(String s) {
        return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
    }
}
