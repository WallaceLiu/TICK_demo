package me.itzg.kapakafkastress.services;

import static java.util.Map.entry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import me.itzg.kapakafkastress.config.KapacitorProperties;
import me.itzg.kapakafkastress.model.Scenario.FieldValue;
import me.itzg.kapakafkastress.model.Scenario.TaskDefinition;
import me.itzg.kapakafkastress.types.ActiveScenario;
import me.itzg.kapakafkastress.types.KapacitorClientException;
import me.itzg.kapakafkastress.types.kapa.DbRp;
import me.itzg.kapakafkastress.types.kapa.KapacitorTask;
import me.itzg.kapakafkastress.types.kapa.KapacitorTask.Status;
import me.itzg.kapakafkastress.types.kapa.Var;
import me.itzg.kapakafkastress.types.kapa.Var.Type;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class KapacitorClient {

  private static final String TAG_RESOURCE = "resource";
  private static final String TAG_SCENARIO = "scenario";
  private static final String FIELD_ITERATION = "iteration";

  private final WebClient kapacitorWebClient;
  private final KapacitorProperties kapacitorProperties;
  private final InfluxDB influxDbClient;

  @Autowired
  public KapacitorClient(KapacitorProperties kapacitorProperties,
                         WebClient.Builder webClientBuilder) {
    this.kapacitorProperties = kapacitorProperties;
    this.kapacitorWebClient = webClientBuilder.baseUrl(kapacitorProperties.getBaseUrl())
        .build();

    influxDbClient = InfluxDBFactory.connect(kapacitorProperties.getBaseUrl())
        .setDatabase(kapacitorProperties.getDb())
        .setRetentionPolicy(kapacitorProperties.getRp());

    influxDbClient.disableBatch();
  }


  public Mono<ActiveScenario> createKapacitorTask(ActiveScenario scenario) {

    final TaskDefinition taskDefinition = scenario.getScenario().getTaskDefinition();
    final Map<String, Var> vars = Map.ofEntries(
        entry("measurement", Var.builder()
            .type(Type.STRING)
            .value(taskDefinition.getMeasurement())
            .build()),
        entry("crit", Var.builder()
            .type(Type.LAMBDA)
            .value(taskDefinition.getCritExpression())
            .build()
        ),
        entry("where_filter", Var.builder()
            .type(Type.LAMBDA)
            .value(String.format("\"scenario\" == '%s'", scenario.getId()))
            .build()
        )
    );

    final KapacitorTask task = KapacitorTask.builder()
        .id(scenario.getId())
        .dbrps(Collections.singletonList(
            DbRp.builder()
                .db(kapacitorProperties.getDb())
                .rp(kapacitorProperties.getRp())
                .build()
        ))
        .templateId(kapacitorProperties.getTemplateId())
        .status(Status.enabled)
        .vars(vars)
        .build();

    log.info("Creating kapacitor task={}", task);
    return kapacitorWebClient.post()
        .uri("/kapacitor/v1/tasks")
        .syncBody(task)
        .accept(MediaType.APPLICATION_JSON)
        .retrieve()
        .onStatus(HttpStatus::is4xxClientError, clientResponse ->
            clientResponse.bodyToMono(KapacitorTask.class)
                .map(KapacitorTask::getError)
                .map(KapacitorClientException::new))
        .bodyToMono(KapacitorTask.class)
        .map(scenario::setKapacitorTask);
  }

  public Mono<String> deleteKapacitorTask(String id) {
    return kapacitorWebClient.delete()
        .uri("/kapacitor/v1/tasks/{id}", id)
        .exchange()
        .doOnSuccess(clientResponse -> {
          log.debug("Deleted kapacitor task={}", id);
        })
        .map(clientResponse -> id);
  }

  public void sendMetric(String activeScenarioId, String resource, String measurementName,
                         Map<String, List<FieldValue>> fieldValues, int iteration) {
    log.debug("Sending metric for scenario={} resource={} measurement={} iteration={}",
        activeScenarioId, resource, measurementName, iteration);

    final Builder pointBuilder = Point.measurement(measurementName)
        .tag(TAG_SCENARIO, activeScenarioId)
        .tag(TAG_RESOURCE, resource)
        .addField(FIELD_ITERATION, iteration);

    for (Entry<String, List<FieldValue>> fieldEntry : fieldValues.entrySet()) {
      final String fieldName = fieldEntry.getKey();

      final List<FieldValue> values = fieldEntry.getValue();
      final FieldValue fieldValue = values.get(iteration % values.size());

      pointBuilder.addField(fieldName, fieldValue.getValue());
    }

    influxDbClient.write(pointBuilder.build());
  }
}
