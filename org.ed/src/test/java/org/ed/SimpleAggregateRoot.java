package org.ed;

public class SimpleAggregateRoot extends AggregateRoot
{
	private static final long serialVersionUID = -4945070453477512209L;

	int i = 0;
	@Override
	public boolean isComplete() {
		return false;
	}
	
	public void add()
	{
		apply(new AddEventMessage());
	}
	
	void on(AddEventMessage message)
	{
		i++;
	}
	
	public int getI()
	{
		return i;
	}
}
