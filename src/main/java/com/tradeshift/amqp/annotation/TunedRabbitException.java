package com.tradeshift.amqp.annotation;

import java.util.Collections;
import java.util.List;

import org.springframework.amqp.core.Message;

public class TunedRabbitException extends RuntimeException {

	private List<Message> toRetry = Collections.emptyList();
	private List<Message> toDlq = Collections.emptyList();
	
	public TunedRabbitException(String message, List<Message> toRetry, List<Message> toDlq) {
		super(message);
		this.toRetry = toRetry;
		this.toDlq = toDlq;
	}
	
	public List<Message> getToRetry() {
		return toRetry;
	}
	
	public void setToRetry(List<Message> toRetry) {
		this.toRetry = toRetry;
	}
	
	public List<Message> getToDlq() {
		return toDlq;
	}
	
	public void setToDlq(List<Message> toDlq) {
		this.toDlq = toDlq;
	}
}
