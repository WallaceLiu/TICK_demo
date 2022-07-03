package me.itzg.kapakafkastress.types.kapa;

import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class SeriesItem {
	private List<String> columns;
	private List<List<Object>> values;
	private String name;
	private Map<String, String> tags;
}