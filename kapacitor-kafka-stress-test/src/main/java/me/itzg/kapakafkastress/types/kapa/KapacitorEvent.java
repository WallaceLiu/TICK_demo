package me.itzg.kapakafkastress.types.kapa;

import lombok.Data;

@Data
public class KapacitorEvent{
	private long duration;
	private String previousLevel;
	private KapacitorEventData data;
	private String level;
	private boolean recoverable;
	private String details;
	private String id;
	private String time;
	private String message;
}
