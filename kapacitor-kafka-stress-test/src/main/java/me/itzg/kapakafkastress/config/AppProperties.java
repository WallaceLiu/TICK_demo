package me.itzg.kapakafkastress.config;

import java.time.Duration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("app")
@Component
@Data
public class AppProperties {

  Duration shutdownTaskDeletionTimeout = Duration.ofSeconds(10);

  Duration delayTaskDeletionOnScheduleComplete = Duration.ofSeconds(5);

  Duration delayFirstMetrics = Duration.ofSeconds(5);
}
