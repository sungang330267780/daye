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
		AggregateTypeCache.registerAggregate(this.getClass());
		for (SubAggregate subAgg : subAggSet) {
			subAgg.init();
			subAgg.setParent(this);
		}
	}

	public Set<SubAggregate> getSubAggregates() {
		return Collections.unmodifiableSet(subAggSet);
	}

	protected void apply(EventMessage event) {
		AggregateTypeCache.invoke(this, event);

		for (SubAggregate subAgg : subAggSet) {
			AggregateTypeCache.invoke(subAgg, event);
		}

		if (repository != null) {
			repository.addEvent(id, event);
			// TODO:以后加入事件发布代码
		}
	}

	public abstract boolean isComplete();

	public void registerSubAggregate(SubAggregate sa) {

		if (subAggSet.contains(sa))
			return;

		sa.init();
		sa.setParent(this);
		subAggSet.add(sa);
	}

	public void unregisterSubAggregate(SubAggregate sa) {
		if (!subAggSet.contains(sa))
			return;

		subAggSet.remove(sa);
		sa.setParent(null);
	}
}
