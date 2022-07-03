package me.itzg.kapakafkastress.services;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import me.itzg.kapakafkastress.config.AppProperties;
import me.itzg.kapakafkastress.model.Scenario;
import me.itzg.kapakafkastress.model.Scenario.Measurement;
import me.itzg.kapakafkastress.types.ActiveScenario;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.listeners.SchedulerListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class ScenariosService {

  private final Scheduler scheduler;
  private final AppProperties appProperties;
  private final KapacitorClient kapacitorClient;

  private ConcurrentHashMap<String, ActiveScenario> activeScenarios = new ConcurrentHashMap<>();

  @Autowired
  public ScenariosService(Scheduler scheduler,
                          AppProperties appProperties,
                          KapacitorClient kapacitorClient) {
    this.scheduler = scheduler;
    this.appProperties = appProperties;
    this.kapacitorClient = kapacitorClient;
  }

  @PostConstruct
  public void setup() throws SchedulerException {
    scheduler.getListenerManager().addSchedulerListener(new SchedulerListenerSupport() {
      @Override
      public void triggerFinalized(Trigger trigger) {
        handleTriggerFinalized(trigger);
      }

    });

    scheduler.getContext().put("scenariosService", this);
    scheduler.getContext().put("kapacitorClient", kapacitorClient);
  }

  @PreDestroy
  public void stop() {
    deleteAll()
        .doOnComplete(() -> log.info("Removed kapacitor tasks"))
        .blockLast(appProperties.getShutdownTaskDeletionTimeout());
  }

  public Mono<String> create(Scenario scenario) {

    return kapacitorClient.createKapacitorTask(new ActiveScenario()
        .setId(UUID.randomUUID().toString())
        .setScenario(scenario)
    )
        .flatMap(activeScenario -> {
          final SimpleScheduleBuilder scheduleBuilder = simpleSchedule()
              .withIntervalInMilliseconds(scenario.getInterval().toMillis());
          if (scenario.getRepeat() > 0) {
            scheduleBuilder.withRepeatCount(scenario.getRepeat()-1);
          } else {
            scheduleBuilder.repeatForever();
          }

          final String id = activeScenario.getId();

          try {

            // Schedule a job per measurement with all jobs being grouped by active scenario ID

            for (Entry<String, Measurement> measurementEntry : scenario.getInput()
                .getMeasurements().entrySet()) {

              final String measurementName = measurementEntry.getKey();

              scheduler.scheduleJob(
                  newJob(ScenarioJob.class)
                      .withIdentity(measurementName, id)
                      .usingJobData(ScenarioJob.ITERATION, 0)
                      .build(),
                  newTrigger()
                      .withIdentity(measurementName, id)
                      .startAt(Date.from(Instant.now().plus(appProperties.getDelayFirstMetrics())))
                      .withSchedule(scheduleBuilder)
                      .build()
              );
            }
          } catch (SchedulerException e) {

            unscheduleActiveScenario(id);

            // and revert the creation of the kapacitor task
            return kapacitorClient.deleteKapacitorTask(id)
                .then(Mono.error(new RuntimeException("Trying to schedule job", e)));
          }

          activeScenarios.put(id, activeScenario);

          log.info("Created task and scheduled scenario {}", id);

          return Mono.just(activeScenario);
        })
        .map(ActiveScenario::getId);
  }

  private void unscheduleActiveScenario(String id) {
    try {

      // unschedule the ones that might have been scheduled
      for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.groupEquals(id))) {
        scheduler.deleteJob(jobKey);
      }

    } catch (SchedulerException ex) {
      log.warn("Unable to locate jobs of group={} to delete them during scheduler failure", id);
    }
  }

  public ActiveScenario getActiveScenario(String activeScenarioId) {
    return activeScenarios.get(activeScenarioId);
  }

  private void handleTriggerFinalized(Trigger trigger) {
    log.debug("Trigger={} finalized", trigger);

    try {
      final String scenarioId = trigger.getJobKey().getGroup();
      boolean anyRunning = false;

      for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.groupEquals(
          scenarioId))) {
        for (Trigger otherTrigger : scheduler.getTriggersOfJob(jobKey)) {
          if (otherTrigger.mayFireAgain()) {
            anyRunning = true;
          }
        }
      }

      if (!anyRunning) {
        log.debug("All triggers have completed of scenario={}", scenarioId);
        Mono.empty()
            .then(delete(scenarioId))
            .delaySubscription(appProperties.getDelayTaskDeletionOnScheduleComplete())
            .subscribe();
      }
    } catch (SchedulerException e) {
      log.warn("Unable to get triggers of job while handling finalized trigger={}", trigger, e);
    }
  }

  public Mono<String> delete(String scenarioId) {
    return kapacitorClient.deleteKapacitorTask(scenarioId)
        .doOnSuccess(aVoid -> {
          activeScenarios.remove(scenarioId);
          try {
            scheduler.deleteJobs(List.copyOf(scheduler.getJobKeys(GroupMatcher.groupEquals(scenarioId))));
          } catch (SchedulerException e) {
            log.warn("Failed to find scheduler jobs while deleting scenario={}", scenarioId, e);
          }
          log.info("Stopped and removed scenario={}", scenarioId);
        });
  }

  public Flux<String> deleteAll() {
    final List<Mono<String>> deletions = activeScenarios.keySet().stream()
        .peek(this::unscheduleActiveScenario)
        .map(kapacitorClient::deleteKapacitorTask)
        .collect(Collectors.toList());

    return Flux.concat(deletions);
  }
}
