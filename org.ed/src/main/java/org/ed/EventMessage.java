package org.ed;

import java.io.Serializable;
import java.util.Date;

public class EventMessage implements Serializable {
	private static final long serialVersionUID = -7424071454218566287L;
	private Date messageDate = new Date();
	private String aggRootId;

	public Date getMessageDate() {
		return messageDate;
	}

	public String getAggRootId() {
		return aggRootId;
	}

	void setAggRootId(String aggRootId) {
		this.aggRootId = aggRootId;
	}
}
