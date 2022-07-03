package me.itzg.kapakafkastress.types.kapa;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KapacitorTask {
  String id;
  Type type;
  List<DbRp> dbrps;
  @JsonProperty("template-id")
  String templateId;
  String script;
  Map<String, Var> vars;
  Status status;
  String error;

  public enum Type {
    stream
  }

  public enum Status {
    enabled, disabled
  }
}
