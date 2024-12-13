package com.tradeshift.amqp.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.test.annotation.DirtiesContext;

import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;
import com.tradeshift.amqp.rabbit.retry.QueueRetryComponent;

@ExtendWith(MockitoExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class EnableRabbitRetryAfterThrowTunedRabbitExceptionAspectTest {

	private static final TunedRabbitProperties createQueueProperties = createQueueProperties();
	
	@Mock
	private TunedRabbitPropertiesMap tunnedRabbitPropertiesMap;
	@Mock
	private QueueRetryComponent queueComponent;
	@Mock
	private MethodSignature signature;
	
	@InjectMocks
	@Spy
	private EnableRabbitRetryAfterThrowTunedRabbitExceptionAspect aspect;
	
	private final Map<String, Method> methods = Arrays.stream(EnableRabbitRetryAfterThrowTunedRabbitExceptionAspectTest.class.getDeclaredMethods())
			.collect(Collectors.toMap(m -> m.getName(), Function.identity()));
	
	@Test
	@EnableRabbitRetryAfterThrowTunedRabbitException(event = "some-event")
	void should_not_send_messages_to_retry_and_dlq() throws Throwable {
		when(tunnedRabbitPropertiesMap.get("some-event")).thenReturn(createQueueProperties);
		
		TunedRabbitException tunnedRabbitException = new TunedRabbitException("Some error", 
				Collections.emptyList(), 
				Collections.emptyList());
		
		ProceedingJoinPoint joinPoint = mockJointPointWithThrowing(
				"should_not_send_messages_to_retry_and_dlq", tunnedRabbitException);

		aspect.retryOrDlqAfterTwrow(joinPoint);

		verify(tunnedRabbitPropertiesMap).get("some-event");
		verifyNoInteractions(queueComponent);
	}
	
	@Test
	@EnableRabbitRetryAfterThrowTunedRabbitException(event = "some-event")
	void should_send_messages_to_retry_and_dlq() throws Throwable {
		when(tunnedRabbitPropertiesMap.get("some-event")).thenReturn(createQueueProperties);
		
		List<Message> dlqMockMessages = List.of(new Message("some-dlq1".getBytes()), new Message("some-dlq2".getBytes()));
		
		TunedRabbitException tunnedRabbitException = new TunedRabbitException("Some error", 
				List.of(new Message("some-retry1".getBytes())),
				dlqMockMessages);
		
		ProceedingJoinPoint joinPoint = mockJointPointWithThrowing(
				"should_send_messages_to_retry_and_dlq", tunnedRabbitException);
		
		aspect.retryOrDlqAfterTwrow(joinPoint);
		
		ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
		ArgumentCaptor<TunedRabbitProperties> propertiesCaptor = ArgumentCaptor.forClass(TunedRabbitProperties.class);
		verify(queueComponent, times(2)).sendToDlq(messageCaptor.capture(), propertiesCaptor.capture());
		
    	List<Message> messages = messageCaptor.getAllValues().stream().toList();
    	assertEquals(dlqMockMessages, messages);
    	
    	List<TunedRabbitProperties> properties = propertiesCaptor.getAllValues().stream().toList();
    	assertFalse(properties.isEmpty());
    	properties.forEach(prop -> assertEquals(prop, createQueueProperties));
    	
		verify(tunnedRabbitPropertiesMap).get("some-event");
		verify(queueComponent, times(1)).sendToRetryOrDlq(Mockito.any(Message.class), Mockito.eq(createQueueProperties));
	}
	
	@Test
	@EnableRabbitRetryAfterThrowTunedRabbitException(event = "some-event")
	void should_send_messages_only_to_retry() throws Throwable {
		when(tunnedRabbitPropertiesMap.get("some-event")).thenReturn(createQueueProperties);
		
		List<Message> retryMockMessages = List.of(new Message("some-retry1".getBytes()), new Message("some-retry2".getBytes()));
		
		TunedRabbitException tunnedRabbitException = new TunedRabbitException("Some error", 
				retryMockMessages,
				Collections.emptyList());
		
		ProceedingJoinPoint joinPoint = mockJointPointWithThrowing(
				"should_send_messages_only_to_retry", tunnedRabbitException);
		
		aspect.retryOrDlqAfterTwrow(joinPoint);
		
		ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
		ArgumentCaptor<TunedRabbitProperties> propertiesCaptor = ArgumentCaptor.forClass(TunedRabbitProperties.class);
		verify(queueComponent, times(2)).sendToRetryOrDlq(messageCaptor.capture(), propertiesCaptor.capture());
		
		List<Message> messages = messageCaptor.getAllValues().stream().toList();
		assertEquals(retryMockMessages, messages);
		
		List<TunedRabbitProperties> properties = propertiesCaptor.getAllValues().stream().toList();
		assertFalse(properties.isEmpty());
		properties.forEach(prop -> assertEquals(prop, createQueueProperties));
		
		verify(tunnedRabbitPropertiesMap).get("some-event");
		verify(queueComponent, times(0)).sendToDlq(Mockito.any(Message.class), Mockito.eq(createQueueProperties));
	}
	
	@Test
	@EnableRabbitRetryAfterThrowTunedRabbitException(event = "some-event")
	void should_send_messages_only_to_dlq() throws Throwable {
		when(tunnedRabbitPropertiesMap.get("some-event")).thenReturn(createQueueProperties);
		
		List<Message> dlqMockMessages = List.of(new Message("some-dlq1".getBytes()), new Message("some-dlq2".getBytes()));
		
		TunedRabbitException tunnedRabbitException = new TunedRabbitException("Some error", 
				Collections.emptyList(),
				dlqMockMessages);
		
		ProceedingJoinPoint joinPoint = mockJointPointWithThrowing(
				"should_send_messages_only_to_dlq", tunnedRabbitException);
		
		aspect.retryOrDlqAfterTwrow(joinPoint);
		
		ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
		ArgumentCaptor<TunedRabbitProperties> propertiesCaptor = ArgumentCaptor.forClass(TunedRabbitProperties.class);
		verify(queueComponent, times(2)).sendToDlq(messageCaptor.capture(), propertiesCaptor.capture());
		
		List<Message> messages = messageCaptor.getAllValues().stream().toList();
		assertEquals(dlqMockMessages, messages);
		
		List<TunedRabbitProperties> properties = propertiesCaptor.getAllValues().stream().toList();
		assertFalse(properties.isEmpty());
		properties.forEach(prop -> assertEquals(prop, createQueueProperties));
		
		verify(tunnedRabbitPropertiesMap).get("some-event");
		verify(queueComponent, times(0)).sendToRetryOrDlq(Mockito.any(Message.class), Mockito.eq(createQueueProperties));
	}
	
	@Test
	@EnableRabbitRetryAfterThrowTunedRabbitException(event = "some-event")
	void should_do_nothing_with_unknown_exception() throws Throwable {
		RuntimeException runtimeException = new RuntimeException("Some error");
		
		ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
		when(joinPoint.proceed()).thenThrow(runtimeException);

		assertEquals("Some error", assertThrows(RuntimeException.class, () -> aspect.retryOrDlqAfterTwrow(joinPoint)).getMessage());

		verifyNoInteractions(tunnedRabbitPropertiesMap);
		verifyNoInteractions(queueComponent);
		verifyNoInteractions(signature);
	}
	
	@Test
	@EnableRabbitRetryAfterThrowTunedRabbitException(event = "some-event")
	void should_do_nothing_when_no_exception_is_throw() throws Throwable {
		ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
		when(joinPoint.proceed()).thenReturn(null);
		
		aspect.retryOrDlqAfterTwrow(joinPoint);
		
		verifyNoInteractions(tunnedRabbitPropertiesMap);
		verifyNoInteractions(queueComponent);
		verifyNoInteractions(signature);
	}
	
	@Test
	@EnableRabbitRetryAfterThrowTunedRabbitException(event = "not-found-event")
	void should_throw_no_such_bean_definition_exception() throws Throwable {
		when(tunnedRabbitPropertiesMap.get("not-found-event")).thenReturn(null);
		
		TunedRabbitException tunnedRabbitException = new TunedRabbitException("Some error", 
				Collections.emptyList(), 
				Collections.emptyList());
		
		ProceedingJoinPoint joinPoint = mockJointPointWithThrowing(
				"should_throw_no_such_bean_definition_exception", tunnedRabbitException);
		
		NoSuchBeanDefinitionException ex = assertThrows(NoSuchBeanDefinitionException.class,
				() -> aspect.retryOrDlqAfterTwrow(joinPoint));
		
		assertEquals("No bean named 'Any bean with name not-found-event was found' available", ex.getMessage());
		
		verify(tunnedRabbitPropertiesMap).get("not-found-event");
		verifyNoInteractions(queueComponent);
	}
	
	private ProceedingJoinPoint mockJointPointWithThrowing(String testMethodName, Exception exceptionToThrown) throws Throwable {
		ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
		
		Method method = mockMethodUsingTestingMethod(testMethodName);
		when(signature.getMethod()).thenReturn(method);
		when(joinPoint.getSignature()).thenReturn(signature);
		
		when(joinPoint.proceed()).thenThrow(exceptionToThrown);
		
		return joinPoint;
	}
	
	private Method mockMethodUsingTestingMethod(String testingMethodName)
			throws NoSuchMethodException, SecurityException {
		return methods.get(testingMethodName);
	}

	
	private static TunedRabbitProperties createQueueProperties() {
		TunedRabbitProperties queueProperties = new TunedRabbitProperties();
		queueProperties.setQueue("queue.test");
		queueProperties.setExchange("ex.test");
		queueProperties.setExchangeType("topic");
		queueProperties.setQueueRoutingKey("routing.key.test");
		queueProperties.setTtlRetryMessage(5000);
		queueProperties.setPrimary(true);
		queueProperties.setVirtualHost("vh");
		queueProperties.setUsername("guest");
		queueProperties.setPassword("guest");
		queueProperties.setHost("host");
		queueProperties.setPort(5672);
		queueProperties.setSslConnection(false);
		queueProperties.setTtlMultiply(1);
		queueProperties.setMaxRetriesAttempts(3);
		return queueProperties;
	}

}
