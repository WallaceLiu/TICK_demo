package me.itzg.kapakafkastress.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("kapacitor")
@Component
@Data
public class KapacitorProperties {
  String baseUrl = "http://localhost:9992";
  String db = "default";
  String rp = "autogen";
  String templateId = "main";
  /**
   * Aligns with topic configured in kapacitor-config/load/handlers/kakfa-events.yml
   */
  String eventsTopic = "events.json";
}
