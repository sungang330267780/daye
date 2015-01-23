package org.ed;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class EventHandlerTypeCache {
	private static Map<Class<?>, Map<Class<?>, Method>> eventHandleMethodCache = new HashMap<Class<?>, Map<Class<?>, Method>>();

	static void registerHandler(Class<?> t) {
		if (eventHandleMethodCache.containsKey(t))
			return;

		Map<Class<?>, Method> methodMap = new HashMap<Class<?>, Method>();
		eventHandleMethodCache.put(t, methodMap);
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
		List<Method> list = getMethods(obj.getClass(), event.getClass());

		for (Method m : list) {
			try {
				m.invoke(obj, event);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			}
		}
	}

	static List<Method> getMethods(Class<?> eventHandlerType, Class<?> evtType) {
		List<Method> list = new ArrayList<Method>();
		if (eventHandleMethodCache.containsKey(eventHandlerType)) {
			Map<Class<?>, Method> evtHandleMethods = eventHandleMethodCache.get(eventHandlerType);
			for (Class<?> type : evtHandleMethods.keySet()) {
				if (type.isAssignableFrom(evtType)) {
					list.add(evtHandleMethods.get(type));
				}
			}
		}

		return list;
	}
}