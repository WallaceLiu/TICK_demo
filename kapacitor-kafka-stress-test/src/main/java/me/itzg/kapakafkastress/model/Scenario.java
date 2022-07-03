package me.itzg.kapakafkastress.model;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.convert.DurationUnit;

@Data
public class Scenario {

  @Min(1)
  int resourceCount = 1;

  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration interval = Duration.ofSeconds(10);

  int repeat = 0;

  @NotNull
  Input input;

  @NotNull
  TaskDefinition taskDefinition;

  @Data
  public static class Input {
    @NotEmpty
    Map<String, Measurement> measurements;
  }

  @Data
  public static class Measurement {
    @NotEmpty
    Map<String, List<FieldValue>> fieldValues;
  }

  @Data
  public static class FieldValue {
    @NotNull
    Number value;
  }

  @Data
  public static class TaskDefinition {
    @NotBlank
    String measurement;
    @NotBlank
    String critExpression;
  }
}
