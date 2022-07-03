package me.itzg.kapakafkastress.types.kapa;

import java.util.List;
import lombok.Data;

@Data
public class KapacitorEventData {
	private List<SeriesItem> series;
}