package com.tradeshift.amqp.annotation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;
import com.tradeshift.amqp.rabbit.retry.QueueRetryComponent;

@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class EnableRabbitRetryAndDlqAspectTest {
	private static final String X_DEATH = "x-death";
	private static final String COUNT = "count";

	private static final TunedRabbitProperties createQueueProperties = createQueueProperties();

	@Mock
	private TunedRabbitPropertiesMap tunnedRabbitPropertiesMap;
	@Mock
	private QueueRetryComponent queueComponent;
	@Mock
	private MethodSignature signature;

	@InjectMocks
	@Spy
	private EnableRabbitRetryAndDlqAspect aspect;
	
	private final Map<String, Method> methods = Arrays.stream(EnableRabbitRetryAndDlqAspectTest.class.getDeclaredMethods())
			.collect(Collectors.toMap(Method::getName, Function.identity()));

	@BeforeEach
	void beforeEach() {
		when(tunnedRabbitPropertiesMap.get("some-event")).thenReturn(createQueueProperties);
		
		// replicates the method because part of it is not visible
		when(queueComponent.countDeath(any(Message.class))).thenAnswer((invocation) -> {
				Message message = invocation.getArgument(0);
		        int count = 0;
		        final Map<String, Object> headers = message.getMessageProperties().getHeaders();
		        if (headers.containsKey(X_DEATH)) {
		        	final List list = (List) Collections.singletonList(headers.get(X_DEATH)).get(0);
		            count = Integer.parseInt(((Map) list.get(0)).get(COUNT).toString());
		        }
		        return ++count;
			});
		
		doCallRealMethod().when(queueComponent).sendToRetryOrDlq(any(Message.class), any());
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event")
	void should_send_to_retry_with_default_config_and_backwards_compatibility() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_to_retry_with_default_config_and_backwards_compatibility", 1, RuntimeException.class);

		aspect.validateMessage(joinPoint);

		verifyIfSentToRetryOrDlqWasCalled(1);
		verifyIfRetryWasCalled(1, 2);
		verifyIfDlqWasCalled(0);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event", retryWhen = NumberFormatException.class)
	void should_send_to_retry_when_exceptions_contains_exception_thrown() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_to_retry_when_exceptions_contains_exception_thrown", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfSentToRetryOrDlqWasCalled(1);
		verifyIfRetryWasCalled(1, 2);
		verifyIfDlqWasCalled(0);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event", retryWhen = NumberFormatException.class)
	void should_send_to_dlq_when_maximum_number_retries_exceeds() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_to_dlq_when_maximum_number_retries_exceeds", 6, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfSentToRetryOrDlqWasCalled(1);
		verifyIfRetryWasCalled(0, 0);
		verifyIfDlqWasCalled(1);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event", retryWhen = NumberFormatException.class)
	void should_send_to_retry_when_retryWhen_contains_exception_thrown() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_to_retry_when_retryWhen_contains_exception_thrown", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfSentToRetryOrDlqWasCalled(1);
		verifyIfRetryWasCalled(1, 2);
		verifyIfDlqWasCalled(0);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event", checkInheritance = true, retryWhen = IllegalArgumentException.class)
	void should_send_to_retry_when_retryWhen_contains_exception_checking_inheritance() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_to_retry_when_retryWhen_contains_exception_checking_inheritance", 1,
				NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfSentToRetryOrDlqWasCalled(1);
		verifyIfRetryWasCalled(1, 2);
		verifyIfDlqWasCalled(0);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event",
		discardWhen = NumberFormatException.class,
		retryWhen = NumberFormatException.class,
		directToDlqWhen = NumberFormatException.class
	)
	void should_send_discard_even_when_retryWhen_contains_same_exception() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_discard_even_when_retryWhen_contains_same_exception", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfTheMessageWasDiscarded();
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event",
		discardWhen = IllegalArgumentException.class,
		retryWhen = NumberFormatException.class,
		directToDlqWhen = NumberFormatException.class,
			checkInheritance = false
	)
	void should_discard_even_when_retryWhen_and_directToDlqWhen_contains_same_exception() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_discard_even_when_retryWhen_and_directToDlqWhen_contains_same_exception", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfDlqWasCalled(1);
		verifySentToRetryNeverCalled();
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event",
		discardWhen = NullPointerException.class,
		retryWhen = IllegalArgumentException.class,
		directToDlqWhen = NumberFormatException.class
	)
	void should_send_dlq_when_only_directToDlqWhen_exceptions_contains() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_dlq_when_only_directToDlqWhen_exceptions_contains", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifySentToRetryNeverCalled();
		verifyIfDlqWasCalled(1);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event", checkInheritance = true,
		discardWhen = NullPointerException.class,
		retryWhen = IllegalStateException.class,
		directToDlqWhen = IllegalArgumentException.class
	)
	void should_send_dlq_when_only_directToDlqWhen_exceptions_contains_checking_inheritance() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_dlq_when_only_directToDlqWhen_exceptions_contains_checking_inheritance", 1,
				NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifySentToRetryNeverCalled();
		verifyIfDlqWasCalled(1);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event",
		directToDlqWhen = NumberFormatException.class
	)
	void should_send_dlq_when_only_directToDlqWhen_exceptions_contains_and_no_other_defined() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_dlq_when_only_directToDlqWhen_exceptions_contains_and_no_other_defined", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifySentToRetryNeverCalled();
		verifyIfDlqWasCalled(1);
	}

	@Test
	@DirectToDqlWhenNumberFormatExceptionListener
	void should_send_dlq_when_only_directToDlqWhen_exceptions_contains_and_no_other_defined_when_it_is_using_a_custom_annotation() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_dlq_when_only_directToDlqWhen_exceptions_contains_and_no_other_defined", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifySentToRetryNeverCalled();
		verifyIfDlqWasCalled(1);
	}
	
	private void verifySentToRetryNeverCalled() {
		verify(queueComponent, never()).sendToRetryOrDlq(any(Message.class), any());
		verify(queueComponent, never()).sendToRetry(any(Message.class), any(), any());
	}

	private void verifyIfTheMessageWasDiscarded() {
		verify(queueComponent, never()).sendToRetryOrDlq(any(Message.class), any());
		verify(queueComponent, never()).sendToRetry(any(Message.class), any(), any());
		verify(queueComponent, never()).sendToDlq(any(Message.class), any());
	}

	private void verifyIfSentToRetryOrDlqWasCalled(int numberOfTimes) {
		verify(queueComponent, times(numberOfTimes)).sendToRetryOrDlq(any(Message.class), eq(createQueueProperties));
	}

	private void verifyIfRetryWasCalled(int numberOfTimes, int deathExpected) {
		verify(queueComponent, times(numberOfTimes)).sendToRetry(any(Message.class), eq(createQueueProperties), eq(deathExpected));
	}

	private void verifyIfDlqWasCalled(int numberOfTimes) {
		verify(queueComponent, times(numberOfTimes)).sendToDlq(any(Message.class), eq(createQueueProperties));
	}

	private ProceedingJoinPoint mockJointPointWithDeathAndThrowing(String testMethodName, int numberOfDeaths,
			Class<? extends Exception> exceptionToThrown) throws Throwable {
		ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
		
		Method method = mockMethodUsingTestingMethod(testMethodName);
		when(signature.getMethod()).thenReturn(method);
		when(joinPoint.getSignature()).thenReturn(signature);
		
		when(joinPoint.getArgs()).thenReturn(new Message[] { createMessageWithDeath(numberOfDeaths) });
		when(joinPoint.proceed()).thenThrow(exceptionToThrown);
		
		return joinPoint;
	}

	private Method mockMethodUsingTestingMethod(String testingMethodName)
			throws NoSuchMethodException, SecurityException {
		
		Method method = methods.get(testingMethodName);
		
		if(Objects.isNull(method)) {
			throw new NoSuchMethodException(String.format("No method with name '%s' found!", testingMethodName));
		}
		
		return method;
	}

	private static Message createMessageWithDeath(int numberOfDeaths) {
		MessageProperties messageProperties = new MessageProperties();
		HashMap<String, Integer> map = new HashMap<>();
		map.put(COUNT, numberOfDeaths);
		messageProperties.getHeaders().put(X_DEATH, Collections.singletonList(map));
		return new Message("some".getBytes(), messageProperties);
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

	

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	@EnableRabbitRetryAndDlq(event = "some-event",
		directToDlqWhen = NumberFormatException.class
	)
	public @interface DirectToDqlWhenNumberFormatExceptionListener {

	}
}
