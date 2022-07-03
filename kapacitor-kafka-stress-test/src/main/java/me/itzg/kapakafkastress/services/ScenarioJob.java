package me.itzg.kapakafkastress.services;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import me.itzg.kapakafkastress.model.Scenario.Measurement;
import me.itzg.kapakafkastress.types.ActiveScenario;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.springframework.scheduling.quartz.QuartzJobBean;

@Slf4j
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class ScenarioJob extends QuartzJobBean {

  public static final String ITERATION = "iteration";

  @Setter
  ScenariosService scenariosService;
  @Setter
  KapacitorClient kapacitorClient;

  @Override
  protected void executeInternal(JobExecutionContext context)
      throws JobExecutionException {
    final JobKey jobKey = context.getJobDetail().getKey();
    final String activeScenarioId = jobKey.getGroup();
    final String measurementName = jobKey.getName();

    final JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
    final int iteration = jobDataMap.getInt(ITERATION);
    jobDataMap.put(ITERATION, iteration+1);

    log.info("Running measurementName={} of activeScenario={}", measurementName, activeScenarioId);

    final ActiveScenario activeScenario = scenariosService.getActiveScenario(activeScenarioId);
    if (activeScenario == null) {
      log.warn("Unable to locate active scenario with id={}", activeScenarioId);
      return;
    }

    for (int i = 0; i < activeScenario.getScenario().getResourceCount(); i++) {
      final String resource = String.format("resource-%d", i);

      final Measurement measurement = activeScenario.getScenario().getInput().getMeasurements()
          .get(measurementName);

      if (measurement != null) {
        kapacitorClient.sendMetric(activeScenarioId,
            resource, measurementName, measurement.getFieldValues(), iteration
        );
      } else {
        log.warn("Could not find measurement={} in scenario", measurement);
      }
    }
  }
}
