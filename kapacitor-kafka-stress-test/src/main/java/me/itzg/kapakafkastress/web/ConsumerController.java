package me.itzg.kapakafkastress.web;

import java.util.Map;
import me.itzg.kapakafkastress.services.KapacitorEventConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

  private final KapacitorEventConsumer kapacitorEventConsumer;

  @Autowired
  public ConsumerController(KapacitorEventConsumer kapacitorEventConsumer) {
    this.kapacitorEventConsumer = kapacitorEventConsumer;
  }

  @GetMapping("/counts")
  public Map<String,Long> getStats() {
    return kapacitorEventConsumer.getCounts();
  }
}
