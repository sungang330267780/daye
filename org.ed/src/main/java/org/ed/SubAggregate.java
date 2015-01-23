package org.ed;


import java.io.Serializable;

public abstract class SubAggregate implements Serializable {
	private static final long serialVersionUID = 3409157327763461007L;
	
	private String id;
	private AggregateRoot aggRoot;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	void init() {
		EventHandlerTypeCache.registerHandler(this.getClass());
	}
	
	protected void Apply(EventMessage event)
	{
		aggRoot.apply(event);
	}
	
	void setAggregateRoot(AggregateRoot aggRoot)
	{
		this.aggRoot = aggRoot;
	}
	
	protected AggregateRoot getAggregateRoot()
	{
		return this.aggRoot;
	}
}