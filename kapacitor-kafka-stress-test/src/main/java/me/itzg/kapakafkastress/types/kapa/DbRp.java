package me.itzg.kapakafkastress.types.kapa;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DbRp {
  String db;
  String rp;
}
