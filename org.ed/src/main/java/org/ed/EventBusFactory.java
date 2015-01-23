package org.ed;

public class EventBusFactory {
	static EventBus defaultEventBus;
	
	public static EventBus getOrCreateEventBus()
	{
		if(defaultEventBus == null)
			defaultEventBus = new DisruptorEventBus();
		return defaultEventBus;
	}
}
