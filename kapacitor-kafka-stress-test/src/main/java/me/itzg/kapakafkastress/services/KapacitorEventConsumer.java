package me.itzg.kapakafkastress.services;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import me.itzg.kapakafkastress.config.KapacitorProperties;
import me.itzg.kapakafkastress.types.kapa.KapacitorEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KapacitorEventConsumer {

  private final KapacitorProperties kapacitorProperties;

  private ConcurrentHashMap<String/*messageKey*/, AtomicLong> counts =
      new ConcurrentHashMap<>();

  @Autowired
  public KapacitorEventConsumer(KapacitorProperties kapacitorProperties) {
    this.kapacitorProperties = kapacitorProperties;
  }

  public String getTopic() {
    return kapacitorProperties.getEventsTopic();
  }

  @KafkaListener(topics = "#{__listener.topic}")
  public void consume(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String messageKey, KapacitorEvent event) {
    final AtomicLong count = counts.computeIfAbsent(messageKey, s -> new AtomicLong(0));
    count.incrementAndGet();

    if (log.isDebugEnabled()) {
      log.debug("Consumed event={} with messageKey={}", event, messageKey);
    } else {
      log.info("Consumed event with messageKey={}", messageKey);
    }
  }

  public Map<String,Long> getCounts() {
    return counts.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> e.getValue().get()
        ));
  }
}
