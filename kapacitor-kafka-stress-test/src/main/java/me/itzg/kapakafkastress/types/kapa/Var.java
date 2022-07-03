package me.itzg.kapakafkastress.types.kapa;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Var {
  Object value;
  Type type;

  public enum Type {
    // can't use lowercase as identifier since those are Java keywords
    @JsonProperty("bool")
    BOOL,
    @JsonProperty("int")
    INT,
    @JsonProperty("float")
    FLOAT,
    @JsonProperty("string")
    STRING,
    @JsonProperty("lambda")
    LAMBDA
  }

  public static Var from(Object value) {
    final VarBuilder builder = Var.builder()
        .value(value);
    if (value instanceof Integer) {
      builder.type(Type.INT);
    } else if (value instanceof Float) {
      builder.type(Type.FLOAT);
    } else if (value instanceof Boolean) {
      builder.type(Type.BOOL);
    } else if (value instanceof String) {
      builder.type(Type.STRING);
    } else {
      throw new IllegalArgumentException("Unable to handle value with type " + value.getClass());
    }
    return builder.build();
  }
}
