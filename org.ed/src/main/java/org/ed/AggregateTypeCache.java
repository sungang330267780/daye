package org.ed;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

class AggregateTypeCache {
	private static Map<Class<?>, Map<Class<?>, Method>> aggOnMethodCache = new HashMap<Class<?>, Map<Class<?>, Method>>();

	static void registerAggregate(Class<?> t) {
		if (aggOnMethodCache.containsKey(t))
			return;

		Map<Class<?>, Method> methodMap = new HashMap<Class<?>, Method>();
		aggOnMethodCache.put(t, methodMap);
		for (Method m : t.getDeclaredMethods()) {
			if (m.getName().equals("on") && m.getParameters().length == 1 && EventMessage.class.isAssignableFrom(m.getParameters()[0].getType())) {
				m.setAccessible(true);
				Class<?> emt = m.getParameters()[0].getType();

				if (!methodMap.containsKey(emt))
					methodMap.put(emt, m);
			}
		}
	}

	static void invoke(Object obj, EventMessage event) {
		Class<?> aggType = obj.getClass();
		Class<?> evtType = event.getClass();

		if (aggOnMethodCache.containsKey(aggType)) {
			Map<Class<?>, Method> evtMethods = aggOnMethodCache.get(aggType);
			for (Class<?> type : evtMethods.keySet()) {
				if (type.isAssignableFrom(evtType)) {
					try {
						evtMethods.get(type).invoke(obj, event);
					} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					}
				}
			}
		}
	}
}