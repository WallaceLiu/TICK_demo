package me.itzg.kapakafkastress.web;

import javax.validation.Valid;
import me.itzg.kapakafkastress.model.Scenario;
import me.itzg.kapakafkastress.services.ScenariosService;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/scenarios")
public class ScenarioController {

  private final ScenariosService scenariosService;

  @Autowired
  public ScenarioController(ScenariosService scenariosService) {
    this.scenariosService = scenariosService;
  }

  @PostMapping
  @ResponseStatus(HttpStatus.CREATED)
  public Mono<String> schedule(@RequestBody @Valid Scenario scenario) throws SchedulerException {
    return scenariosService.create(scenario);
  }

  @DeleteMapping
  public Flux<String> deleteAll() {
    return scenariosService.deleteAll();
  }

  @DeleteMapping("/{id}")
  public Mono<String> delete(@PathVariable String id) {
    return scenariosService.delete(id);
  }
}
