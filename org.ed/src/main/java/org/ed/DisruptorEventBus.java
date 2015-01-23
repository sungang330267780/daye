package org.ed;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorEventBus implements EventBus {
	private Disruptor<DisruptorEvent> disruptor;
	private ExecutorService executor;

	@Override
	public void close() throws IOException {
		if (disruptor != null) {
			try {
				disruptor.shutdown(1, TimeUnit.MINUTES);
			} catch (TimeoutException e) {
			}
			
			executor.shutdownNow();
		}
	}

	@Override
	public void publish(EventMessage... events) {
		for (EventMessage event : events) {
			RingBuffer<DisruptorEvent> ringBuffer = disruptor.getRingBuffer();
			long sequence = ringBuffer.next();
			try {
				DisruptorEvent de = ringBuffer.get(sequence);
				de.event = event;
			} finally {
				ringBuffer.publish(sequence);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void subscribe(Object... eventListeners) {
		DisruptorEventFactory factory = new DisruptorEventFactory();
		int bufferSize = 1024;
		executor = Executors.newCachedThreadPool();
		disruptor = new Disruptor<>(factory, bufferSize, executor);
		
		for(Object obj : eventListeners)
		{
			EventHandlerTypeCache.registerHandler(obj.getClass());
			disruptor.handleEventsWith(new DisruptorEventHandler(obj));
		}
		disruptor.start();
	}

	private class DisruptorEvent {
		EventMessage event;
	}

	private class DisruptorEventFactory implements EventFactory<DisruptorEvent> {
		@Override
		public DisruptorEvent newInstance() {
			return new DisruptorEvent();
		}

	}

	private class DisruptorEventHandler implements EventHandler<DisruptorEvent> {
		Object obj;

		public DisruptorEventHandler(Object obj) {
			this.obj = obj;
		}

		@Override
		public void onEvent(DisruptorEvent event, long sequence, boolean endOfBatch) throws Exception {
			List<Method> list = EventHandlerTypeCache.getMethods(obj.getClass(), event.event.getClass());
			for (Method m : list) {
				m.invoke(obj, event.event);
			}
		}
	}
}