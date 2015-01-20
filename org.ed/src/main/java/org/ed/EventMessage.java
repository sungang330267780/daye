package org.ed;

import java.io.Serializable;
import java.util.Date;

public class EventMessage implements Serializable {
	private static final long serialVersionUID = -7424071454218566287L;
	private Date MessageDate;

	public Date getMessageDate() {
		return MessageDate;
	}

	public void setMessageDate(Date messageDate) {
		MessageDate = messageDate;
	}
}
