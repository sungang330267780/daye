package org.ed;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class AggregateRootTest {
	private CountDownLatch latch = new CountDownLatch(3);

	public class PrivateEventMessageT extends EventMessage {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	}

	public class ProtectedEventMessageT extends EventMessage {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	}

	public class PublicEventMessageT extends EventMessage {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	}

	public class AggregateRootT extends AggregateRoot {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public void on(PublicEventMessageT message) {
			latch.countDown();
		}

		protected void on(ProtectedEventMessageT message) {
			latch.countDown();
		}

		@SuppressWarnings("unused")
		private void on(PrivateEventMessageT message) {
			latch.countDown();
		}

		@Override
		public boolean isComplete() {
			return true;
		}
	}

	@Test
	public void test() {
		AggregateRootT es = new AggregateRootT();
		es.init();

		es.apply(new PublicEventMessageT());
		es.apply(new ProtectedEventMessageT());
		es.apply(new PrivateEventMessageT());

		try {
			if (!latch.await(10, TimeUnit.MILLISECONDS)) {
				Assert.fail("无法调用方法");
			}
		} catch (InterruptedException e) {
		}
	}
}