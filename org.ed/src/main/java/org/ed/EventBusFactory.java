package org.ed;

public class EventBusFactory {
	public static EventBus createEventBus()
	{
		return new DisruptorEventBus();
	}
}
