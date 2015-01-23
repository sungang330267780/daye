package org.ed;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/***
 * 业务聚合根
 * 
 * @author tao
 *
 */
public abstract class AggregateRoot implements Serializable {
	private static final long serialVersionUID = 8265486901848966590L;

	transient private Repository<?> repository;
	private Set<SubAggregate> subAggSet = new HashSet<SubAggregate>();
	private String id;

	public Repository<?> getRepository() {
		return repository;
	}

	public void setRepository(Repository<?> repository) {
		this.repository = repository;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void init() {
		EventHandlerTypeCache.registerHandler(this.getClass());
		for (SubAggregate subAgg : subAggSet) {
			subAgg.init();
			subAgg.setAggregateRoot(this);
		}
	}

	public Set<SubAggregate> getSubAggregates() {
		return Collections.unmodifiableSet(subAggSet);
	}

	protected void apply(EventMessage event) {
		EventHandlerTypeCache.invoke(this, event);

		for (SubAggregate subAgg : subAggSet) {
			EventHandlerTypeCache.invoke(subAgg, event);
		}

		if (repository != null) {
			repository.addEvent(id, event);
			EventBusFactory.getOrCreateEventBus().publish(event);
		}
	}

	public abstract boolean isComplete();

	public void registerSubAggregate(SubAggregate sa) {

		if (subAggSet.contains(sa))
			return;

		sa.init();
		sa.setAggregateRoot(this);
		subAggSet.add(sa);
	}

	public void unregisterSubAggregate(SubAggregate sa) {
		if (!subAggSet.contains(sa))
			return;

		subAggSet.remove(sa);
		sa.setAggregateRoot(null);
	}
}
