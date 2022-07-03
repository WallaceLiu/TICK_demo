package me.itzg.kapakafkastress;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KapacitorKafkaStressTestApplication {

  public static void main(String[] args) {
    SpringApplication.run(KapacitorKafkaStressTestApplication.class, args);
  }

}
