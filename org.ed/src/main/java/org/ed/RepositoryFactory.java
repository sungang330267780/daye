package org.ed;

import java.util.function.Function;

public class RepositoryFactory {
	private static String dir = "d:/es";
	public static <T extends AggregateRoot> Repository<T> getOrCreateRepository(String name, Function<String, T> creator)
	{
		return new ObjectStreamRepository<T>(dir, name, creator, EventBusFactory.getOrCreateEventBus());
	}
}

