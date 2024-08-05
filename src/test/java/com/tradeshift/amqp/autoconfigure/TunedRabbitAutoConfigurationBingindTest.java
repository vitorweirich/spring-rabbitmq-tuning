package com.tradeshift.amqp.autoconfigure;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.tradeshift.amqp.rabbit.components.RabbitComponentsFactory;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;

@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class TunedRabbitAutoConfigurationBingindTest {

	private TunedRabbitAutoConfiguration tradeshiftRabbitAutoConfiguration;
	
    @Autowired
    private GenericApplicationContext context;

    @Autowired
    private ConfigurableListableBeanFactory beanFactory;
    
    @SpyBean
    private RabbitComponentsFactory rabbitComponentsFactory;

	private RabbitAdmin rabbitAdmin;
    
    @BeforeEach
    void setup() {
        initMocks(this);
        tradeshiftRabbitAutoConfiguration = spy(new TunedRabbitAutoConfiguration(context, beanFactory));
        
        when(tradeshiftRabbitAutoConfiguration.rabbitComponentsFactory()).thenReturn(rabbitComponentsFactory);
        
        rabbitAdmin = mock(RabbitAdmin.class);
    	doReturn(rabbitAdmin).when(rabbitComponentsFactory).createRabbitAdminBean(any());
    }
    
    @Test
    void should_create_binding_for_one_event() {
    	
    	TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
    	TunedRabbitProperties eventProperties = createQueuePropertiesWithAutoCreate(true);
    	rabbitCustomPropertiesMap.put("some-event", eventProperties);

    	tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

    	// Validate the single exchange
    	ArgumentCaptor<Exchange> exchangeArgumentCaptor = ArgumentCaptor.forClass(Exchange.class);
    	verify(rabbitAdmin).declareExchange(exchangeArgumentCaptor.capture());
    	assertEquals("ex.test", exchangeArgumentCaptor.getValue().getName());

    	// Validate all the queues
    	ArgumentCaptor<Queue> queueArgumentCaptor = ArgumentCaptor.forClass(Queue.class);
    	verify(rabbitAdmin, times(3)).declareQueue(queueArgumentCaptor.capture());
    	List<String> queuesNames = queueArgumentCaptor.getAllValues().stream()
    			.map(Queue::getName)
    			.collect(toList());
    	
    	assertTrue(queuesNames.contains("queue.test"));
    	assertTrue(queuesNames.contains("queue.test.dlq"));
    	assertTrue(queuesNames.contains("queue.test.retry"));
    }
    
    @Test
    void should_not_create_binding_for_retry_and_dlq_when_disabled() {
    	
    	TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
    	TunedRabbitProperties eventProperties = createQueuePropertiesWithAutoCreate(true);
    	eventProperties.setAutoCreateForRetryDlq(false);
    	rabbitCustomPropertiesMap.put("some-event", eventProperties);

    	tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

    	// Validate the single exchange
    	ArgumentCaptor<Exchange> exchangeArgumentCaptor = ArgumentCaptor.forClass(Exchange.class);
    	verify(rabbitAdmin).declareExchange(exchangeArgumentCaptor.capture());
    	assertEquals("ex.test", exchangeArgumentCaptor.getValue().getName());

    	// Validate all the queues
    	ArgumentCaptor<Queue> queueArgumentCaptor = ArgumentCaptor.forClass(Queue.class);
    	verify(rabbitAdmin).declareQueue(queueArgumentCaptor.capture());
    	assertEquals("queue.test", queueArgumentCaptor.getValue().getName());
    }

    @Test
    void should_create_binding_for_events_with_same_host_port_and_virtualhost() {

    	TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
    	TunedRabbitProperties eventProperties = createQueuePropertiesWithAutoCreate(true);
    	rabbitCustomPropertiesMap.put("some-event", eventProperties);

    	eventProperties = createQueuePropertiesWithAutoCreate(false);
    	eventProperties.setQueue("queue2.test");
    	eventProperties.setExchange("exchange2.test");
    	rabbitCustomPropertiesMap.put("some-event2", eventProperties);

    	tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

    	// Validate the two exchanges
    	ArgumentCaptor<Exchange> exchangeArgumentCaptor = ArgumentCaptor.forClass(Exchange.class);
    	verify(rabbitAdmin, times(2)).declareExchange(exchangeArgumentCaptor.capture());
    	List<String> exchangesNames = exchangeArgumentCaptor.getAllValues().stream()
    			.map(Exchange::getName)
    			.collect(toList());
    	assertTrue(exchangesNames.contains("ex.test"));
    	assertTrue(exchangesNames.contains("exchange2.test"));

    	ArgumentCaptor<Queue> queueArgumentCaptor = ArgumentCaptor.forClass(Queue.class);
    	verify(rabbitAdmin, times(6)).declareQueue(queueArgumentCaptor.capture());
    	List<String> queuesNames = queueArgumentCaptor.getAllValues().stream()
    			.map(Queue::getName)
    			.collect(toList());
    	assertTrue(queuesNames.contains("queue.test"));
    	assertTrue(queuesNames.contains("queue.test.dlq"));
    	assertTrue(queuesNames.contains("queue.test.retry"));
    	assertTrue(queuesNames.contains("queue2.test"));
    	assertTrue(queuesNames.contains("queue2.test.dlq"));
    	assertTrue(queuesNames.contains("queue2.test.retry"));
    }
    
    private TunedRabbitProperties createQueuePropertiesWithAutoCreate(boolean primary) {
    	return createQueuePropertiesWithAutoCreate(primary, "localhost", 5672, null);
    }

    private TunedRabbitProperties createQueuePropertiesWithAutoCreate(boolean primary, String host, int port, String virtualHost) {
    	TunedRabbitProperties queueProperties = new TunedRabbitProperties();
    	queueProperties.setQueue("queue.test");
    	queueProperties.setExchange("ex.test");
    	queueProperties.setExchangeType("topic");
    	queueProperties.setMaxRetriesAttempts(5);
    	queueProperties.setQueueRoutingKey("routing.key.test");
    	queueProperties.setTtlRetryMessage(3000);
    	queueProperties.setPrimary(primary);
    	queueProperties.setUsername("guest");
    	queueProperties.setPassword("guest");
    	queueProperties.setHost(host);
    	queueProperties.setPort(port);
    	queueProperties.setVirtualHost(virtualHost);
    	queueProperties.setSslConnection(false);
    	queueProperties.setEnableJsonMessageConverter(false);
    	queueProperties.setAutoCreate(true);

    	return queueProperties;
    }
}
