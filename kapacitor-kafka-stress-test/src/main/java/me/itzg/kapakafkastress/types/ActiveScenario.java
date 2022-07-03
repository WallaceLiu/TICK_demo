package me.itzg.kapakafkastress.types;

import lombok.Data;
import me.itzg.kapakafkastress.model.Scenario;
import me.itzg.kapakafkastress.types.kapa.KapacitorTask;

@Data
public class ActiveScenario {
  String id;

  Scenario scenario;

  KapacitorTask kapacitorTask;
}
