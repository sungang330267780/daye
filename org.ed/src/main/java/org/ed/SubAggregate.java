package org.ed;


import java.io.Serializable;

public abstract class SubAggregate implements Serializable {
	private static final long serialVersionUID = 3409157327763461007L;
	
	private String id;
	private AggregateRoot parent;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	void init() {
		AggregateTypeCache.registerAggregate(this.getClass());
	}
	
	void Apply(EventMessage event)
	{
		parent.apply(event);
	}
	
	void setParent(AggregateRoot agg)
	{
		this.parent = agg;
	}
}