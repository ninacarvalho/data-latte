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

@Component
public class KafkaEventProducer implements Producer<GithubEvent> {
    public static final String TOPIC = "etl.github.events";

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private final KafkaTemplate<String, GithubEvent> template;

    public KafkaEventProducer(KafkaTemplate<String, GithubEvent> template) {
        this.template = Objects.requireNonNull(template);
    }

    @Override
    public void send(GithubEvent e) {
        var rec = new ProducerRecord<>(TOPIC, e.getId(), e);
        rec.headers()
                .add(new RecordHeader("eventType", bytes(e.getEventType())))
                .add(new RecordHeader("source",    bytes("github")));

        CompletableFuture<SendResult<String, GithubEvent>> fut = template.send(rec);
        fut.whenComplete((res, ex) -> {
            if (ex == null) {
                var md = res.getRecordMetadata();
                log.info("produced key={} topic={} partition={} offset={}",
                        e.getId(), md.topic(), md.partition(), md.offset());
            } else {
                log.error("produce failed key={} reason={}", e.getId(), ex.toString());
            }
        });
    }

    @Override
    public void sendBatch(List<GithubEvent> batch) {
        for (GithubEvent e : batch) send(e);
    }

    private static byte[] bytes(String s) {
        return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
    }

}
