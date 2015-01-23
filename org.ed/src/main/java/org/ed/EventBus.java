package org.ed;

import java.io.Closeable;

public interface EventBus extends Closeable {
	void publish(EventMessage... events);
	void subscribe(Object... eventListeners);
}
