package org.ed;

public interface EventBus {
	void publish(EventMessage... events);
	void subscribe(Object eventListener);
	void unsubscribe(Object eventListener);
}
