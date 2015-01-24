package org.ed;

import java.util.function.Function;

public class RepositoryFactory {
	private static String repositoryRootDir = "d:/es";

	public static <T extends AggregateRoot> Repository<T> createRepository(String name, Function<String, T> creator, EventBus eventBus) {
		return new ObjectStreamRepository<T>(repositoryRootDir, name, creator, eventBus);
	}
}
