package com.tradeshift.amqp.autoconfigure;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import com.rabbitmq.client.Address;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;
import com.tradeshift.amqp.resolvers.RabbitBeanNameResolver;

@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class TunedRabbitAutoConfigurationTest {

    private TunedRabbitAutoConfiguration tradeshiftRabbitAutoConfiguration;

    @Autowired
    private GenericApplicationContext context;

    @Autowired
    private ConfigurableListableBeanFactory beanFactory;
    
    @BeforeEach
    void setup() {
        tradeshiftRabbitAutoConfiguration = new TunedRabbitAutoConfiguration(context, beanFactory);
    }

    @Test
    void should_create_all_beans_for_rabbitmq_architecture() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queueProperties = createQueueProperties(true);
        rabbitCustomPropertiesMap.put("some-event", queueProperties);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactory = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queueProperties));
        RabbitTemplate rabbitTemplate = (RabbitTemplate) context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queueProperties));
        RabbitAdmin rabbitAdmin = (RabbitAdmin) context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queueProperties));
        SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory =
                (SimpleRabbitListenerContainerFactory) context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queueProperties));

        assertNotNull(connectionFactory);
        assertNotNull(rabbitTemplate);
        assertNotNull(rabbitAdmin);
        assertNotNull(simpleRabbitListenerContainerFactory);

        assertEquals("/", connectionFactory.getVirtualHost());
        assertEquals("localhost", connectionFactory.getHost());
        assertEquals(5672, connectionFactory.getPort());
        assertEquals("guest", connectionFactory.getUsername());

        assertEquals(1, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(1, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(1, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(1, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }
    
    @Test
    void should_create_one_listener_container_factory_for_every_event_with_distinct_configuration() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queueProperties = createQueueProperties(true);
        rabbitCustomPropertiesMap.put("distinct-event1-a", queueProperties);
        rabbitCustomPropertiesMap.put("distinct-event1-b", createQueueProperties(false));
        TunedRabbitProperties distinctEvent2Properties = createQueueProperties(false);
        distinctEvent2Properties.setPrefetchCount(1);
        rabbitCustomPropertiesMap.put("distinct-event2-a", distinctEvent2Properties);
        rabbitCustomPropertiesMap.put("distinct-event2-b", distinctEvent2Properties);
        TunedRabbitProperties distinctEvent3Properties = createQueueProperties(false);
        distinctEvent3Properties.setPrefetchCount(1);
        distinctEvent3Properties.setConcurrentConsumers(2);
        distinctEvent3Properties.setMaxConcurrentConsumers(2);
        rabbitCustomPropertiesMap.put("distinct-event3-a", distinctEvent3Properties);
        rabbitCustomPropertiesMap.put("distinct-event3-b", distinctEvent3Properties);
        TunedRabbitProperties distinctEvent4Properties = createQueueProperties(false);
        distinctEvent4Properties.setPrefetchCount(1);
        distinctEvent4Properties.setConcurrentConsumers(2);
        distinctEvent4Properties.setMaxConcurrentConsumers(4);
        rabbitCustomPropertiesMap.put("distinct-event4-a", distinctEvent4Properties);
        rabbitCustomPropertiesMap.put("distinct-event4-b", distinctEvent4Properties);
        TunedRabbitProperties distinctEvent5Properties = createQueueProperties(false);
        distinctEvent5Properties.setPrefetchCount(500);
        distinctEvent5Properties.setBatchListener(true);
        distinctEvent5Properties.setConsumerBatchEnabled(true);
        distinctEvent5Properties.setBatchSize(500);
        distinctEvent5Properties.setReceiveTimeout(1000);
        distinctEvent5Properties.setConcurrentConsumers(2);
        distinctEvent5Properties.setMaxConcurrentConsumers(4);
        rabbitCustomPropertiesMap.put("distinct-event5-a", distinctEvent5Properties);
        rabbitCustomPropertiesMap.put("distinct-event5-b", distinctEvent5Properties);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        assertEquals(5, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
        Map<String, SimpleRabbitListenerContainerFactory> rabbitListenerBeans = context.getBeansOfType(SimpleRabbitListenerContainerFactory.class);
        
        SimpleRabbitListenerContainerFactory distinctEvent1RabbitListener = rabbitListenerBeans.get("containerFactoryDefaultLocalhost5672_1-1-250");
        assertEquals(1, ReflectionTestUtils.getField(distinctEvent1RabbitListener, "concurrentConsumers"));
        assertEquals(1, ReflectionTestUtils.getField(distinctEvent1RabbitListener, "maxConcurrentConsumers"));
        assertEquals(250, ReflectionTestUtils.getField(distinctEvent1RabbitListener, "prefetchCount"));
        
        SimpleRabbitListenerContainerFactory distinctEvent2RabbitListener = rabbitListenerBeans.get("containerFactoryDefaultLocalhost5672_1-1-1");
        assertEquals(1, ReflectionTestUtils.getField(distinctEvent2RabbitListener, "concurrentConsumers"));
        assertEquals(1, ReflectionTestUtils.getField(distinctEvent2RabbitListener, "maxConcurrentConsumers"));
        assertEquals(1, ReflectionTestUtils.getField(distinctEvent2RabbitListener, "prefetchCount"));
        
        SimpleRabbitListenerContainerFactory distinctEvent3RabbitListener = rabbitListenerBeans.get("containerFactoryDefaultLocalhost5672_2-2-1");
        assertEquals(2, ReflectionTestUtils.getField(distinctEvent3RabbitListener, "concurrentConsumers"));
        assertEquals(2, ReflectionTestUtils.getField(distinctEvent3RabbitListener, "maxConcurrentConsumers"));
        assertEquals(1, ReflectionTestUtils.getField(distinctEvent3RabbitListener, "prefetchCount"));
        
        SimpleRabbitListenerContainerFactory distinctEvent4RabbitListener = rabbitListenerBeans.get("containerFactoryDefaultLocalhost5672_2-4-1");
        assertEquals(2, ReflectionTestUtils.getField(distinctEvent4RabbitListener, "concurrentConsumers"));
        assertEquals(4, ReflectionTestUtils.getField(distinctEvent4RabbitListener, "maxConcurrentConsumers"));
        assertEquals(1, ReflectionTestUtils.getField(distinctEvent4RabbitListener, "prefetchCount"));
        
        SimpleRabbitListenerContainerFactory distinctEvent5RabbitListener = rabbitListenerBeans.get("containerFactoryDefaultLocalhost5672_2-4-500-truetrue5001000");
        assertEquals(2, ReflectionTestUtils.getField(distinctEvent5RabbitListener, "concurrentConsumers"));
        assertEquals(4, ReflectionTestUtils.getField(distinctEvent5RabbitListener, "maxConcurrentConsumers"));
        assertEquals(500, ReflectionTestUtils.getField(distinctEvent5RabbitListener, "prefetchCount"));
        assertEquals(1000L, ReflectionTestUtils.getField(distinctEvent5RabbitListener, "receiveTimeout"));
        assertEquals(500, ReflectionTestUtils.getField(distinctEvent5RabbitListener, "batchSize"));
        assertTrue((Boolean) ReflectionTestUtils.getField(distinctEvent5RabbitListener, "consumerBatchEnabled"));
        assertTrue((Boolean) ReflectionTestUtils.getField(distinctEvent5RabbitListener, "batchListener"));
    }
    
    @Test
    void should_create_all_beans_for_rabbitmq_architecture_even_when_container_factory_bean_name_is_provided() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queueProperties = createQueueProperties(true);
        queueProperties.setRabbitContainerFactoryBeanName("some-container-factory-bean-name");
        rabbitCustomPropertiesMap.put("some-event", queueProperties);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactory = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queueProperties));
        RabbitTemplate rabbitTemplate = (RabbitTemplate) context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queueProperties));
        RabbitAdmin rabbitAdmin = (RabbitAdmin) context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queueProperties));
        SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory =
        		(SimpleRabbitListenerContainerFactory) context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queueProperties));

        assertNotNull(connectionFactory);
        assertNotNull(rabbitTemplate);
        assertNotNull(rabbitAdmin);

        assertEquals("/", connectionFactory.getVirtualHost());
        assertEquals("localhost", connectionFactory.getHost());
        assertEquals(5672, connectionFactory.getPort());
        assertEquals("guest", connectionFactory.getUsername());

        assertEquals(1, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(1, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(1, context.getBeansOfType(RabbitAdmin.class).size());
        
        assertNotNull(simpleRabbitListenerContainerFactory);
    }

    @Test
    void should_create_all_beans_for_rabbitmq_architecture_using_json_message_converter() throws IllegalAccessException {
        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queueProperties = createQueueProperties(true, null, true);
        rabbitCustomPropertiesMap.put("some-event", queueProperties);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactory = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queueProperties));
        RabbitTemplate rabbitTemplate = (RabbitTemplate) context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queueProperties));
        RabbitAdmin rabbitAdmin = (RabbitAdmin) context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queueProperties));
        SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory =
                (SimpleRabbitListenerContainerFactory) context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queueProperties));

        assertNotNull(connectionFactory);
        assertNotNull(rabbitTemplate);
        assertNotNull(rabbitAdmin);
        assertNotNull(simpleRabbitListenerContainerFactory);

        assertEquals(Jackson2JsonMessageConverter.class, rabbitTemplate.getMessageConverter().getClass());
        assertEquals(Jackson2JsonMessageConverter.class, getMessageConverter(simpleRabbitListenerContainerFactory).getClass());
    }

    @Test
    void should_create_all_beans_for_rabbitmq_architecture_using_default_message_converter() throws IllegalAccessException {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queueProperties = createQueueProperties(true);
        rabbitCustomPropertiesMap.put("some-event", queueProperties);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactory = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queueProperties));
        RabbitTemplate rabbitTemplate = (RabbitTemplate) context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queueProperties));
        RabbitAdmin rabbitAdmin = (RabbitAdmin) context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queueProperties));
        SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory =
                (SimpleRabbitListenerContainerFactory) context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queueProperties));

        assertNotNull(connectionFactory);
        assertNotNull(rabbitTemplate);
        assertNotNull(rabbitAdmin);
        assertNotNull(simpleRabbitListenerContainerFactory);

        assertEquals(SimpleMessageConverter.class, rabbitTemplate.getMessageConverter().getClass());
        assertEquals(SimpleMessageConverter.class, getMessageConverter(simpleRabbitListenerContainerFactory).getClass());
    }

    @Test
    void should_return_an_excp_because_there_are_2_primaries_definitions() {
        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true));
        rabbitCustomPropertiesMap.put("some-event2", createQueueProperties(true));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap));
        assertEquals("Only one primary RabbitMQ architecture is allowed!", ex.getMessage());
    }

    @Test
    void should_create_2_connectionFactories_and_all_other_beans_for_different_hosts_different_port_and_different_virtual_host() {

        String anotherVirtualHost = "test";
        String anotherUsername = "anotherUsername";
        String anotherHost = "anotherHost";
        int anotherPort = 5670;

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queuePropertiesSomeEvent = createQueueProperties(true);
        rabbitCustomPropertiesMap.put("some-event", queuePropertiesSomeEvent);

        TunedRabbitProperties queuePropertiesAnotherEvent = createQueueProperties(false, anotherVirtualHost, anotherUsername, false, anotherHost, anotherPort);
        rabbitCustomPropertiesMap.put("some-event2", queuePropertiesAnotherEvent);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactoryForDefaultVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent));
        CachingConnectionFactory connectionFactoryForAnotherVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesAnotherEvent));

        assertNotNull(connectionFactoryForDefaultVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queuePropertiesSomeEvent)));

        assertNotNull(connectionFactoryForAnotherVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesAnotherEvent)));

        assertEquals("/", connectionFactoryForDefaultVH.getVirtualHost());
        assertEquals("localhost", connectionFactoryForDefaultVH.getHost());
        assertEquals(5672, connectionFactoryForDefaultVH.getPort());
        assertEquals("guest", connectionFactoryForDefaultVH.getUsername());

        assertEquals(anotherVirtualHost, connectionFactoryForAnotherVH.getVirtualHost());
        assertEquals(anotherHost, connectionFactoryForAnotherVH.getHost());
        assertEquals(anotherPort, connectionFactoryForAnotherVH.getPort());
        assertEquals(anotherUsername, connectionFactoryForAnotherVH.getUsername());

        assertEquals(2, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(2, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(2, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(2, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }

    @Test
    void should_create_2_connectionFactories_and_all_other_beans_for_different_hosts_same_port_and_different_virtual_host() {

        String anotherVirtualHost = "test";
        String anotherUsername = "anotherUsername";
        String anotherHost = "anotherHost";
        int samePort = 5672;

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queuePropertiesSomeEvent = createQueueProperties(true, null, "guest", false, "localhost", samePort);
        rabbitCustomPropertiesMap.put("some-event", queuePropertiesSomeEvent);

        TunedRabbitProperties queuePropertiesAnotherEvent = createQueueProperties(false, anotherVirtualHost, anotherUsername, false, anotherHost, samePort);
        rabbitCustomPropertiesMap.put("some-event2", queuePropertiesAnotherEvent);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactoryForDefaultVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent));
        CachingConnectionFactory connectionFactoryForAnotherVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesAnotherEvent));

        assertNotNull(connectionFactoryForDefaultVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queuePropertiesSomeEvent)));

        assertNotNull(connectionFactoryForAnotherVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesAnotherEvent)));

        assertEquals("/", connectionFactoryForDefaultVH.getVirtualHost());
        assertEquals("localhost", connectionFactoryForDefaultVH.getHost());
        assertEquals(samePort, connectionFactoryForDefaultVH.getPort());
        assertEquals("guest", connectionFactoryForDefaultVH.getUsername());

        assertEquals(anotherVirtualHost, connectionFactoryForAnotherVH.getVirtualHost());
        assertEquals(anotherHost, connectionFactoryForAnotherVH.getHost());
        assertEquals(samePort, connectionFactoryForAnotherVH.getPort());
        assertEquals(anotherUsername, connectionFactoryForAnotherVH.getUsername());

        assertEquals(2, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(2, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(2, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(2, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }

    @Test
    void should_create_2_connectionFactories_and_all_other_beans_for_same_hosts_different_port_and_different_virtual_host() {

        String anotherVirtualHost = "test";
        String anotherUsername = "anotherUsername";
        String sameHost = "localhost";

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queuePropertiesSomeEvent = createQueueProperties(true, null, "guest", false, sameHost, 5673);
        rabbitCustomPropertiesMap.put("some-event", queuePropertiesSomeEvent);

        TunedRabbitProperties queuePropertiesAnotherEvent = createQueueProperties(false, anotherVirtualHost, anotherUsername, false, sameHost, 5672);
        rabbitCustomPropertiesMap.put("some-event2", queuePropertiesAnotherEvent);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactoryForDefaultVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent));
        CachingConnectionFactory connectionFactoryForAnotherVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesAnotherEvent));

        assertNotNull(connectionFactoryForDefaultVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queuePropertiesSomeEvent)));

        assertNotNull(connectionFactoryForAnotherVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesAnotherEvent)));

        assertEquals("/", connectionFactoryForDefaultVH.getVirtualHost());
        assertEquals("localhost", connectionFactoryForDefaultVH.getHost());
        assertEquals(5673, connectionFactoryForDefaultVH.getPort());
        assertEquals("guest", connectionFactoryForDefaultVH.getUsername());

        assertEquals(anotherVirtualHost, connectionFactoryForAnotherVH.getVirtualHost());
        assertEquals(sameHost, connectionFactoryForAnotherVH.getHost());
        assertEquals(5672, connectionFactoryForAnotherVH.getPort());
        assertEquals(anotherUsername, connectionFactoryForAnotherVH.getUsername());

        assertEquals(2, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(2, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(2, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(2, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }

    @Test
    void should_create_2_connectionFactories_and_all_other_beans_for_same_hosts_different_port_and_same_virtual_host() {

        String sameVH = "test";
        String sameUsername = "guest";
        String sameHost = "localhost";

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queuePropertiesSomeEvent = createQueueProperties(true, sameVH, sameUsername, false, sameHost, 5673);
        rabbitCustomPropertiesMap.put("some-event", queuePropertiesSomeEvent);

        TunedRabbitProperties queuePropertiesAnotherEvent = createQueueProperties(false, sameVH, sameUsername, false, sameHost, 5672);
        rabbitCustomPropertiesMap.put("some-event2", queuePropertiesAnotherEvent);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactoryForDefaultVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesSomeEvent));
        CachingConnectionFactory connectionFactoryForAnotherVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesAnotherEvent));

        assertNotNull(connectionFactoryForDefaultVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesSomeEvent)));

        assertNotNull(connectionFactoryForAnotherVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesAnotherEvent)));

        assertEquals(sameVH, connectionFactoryForDefaultVH.getVirtualHost());
        assertEquals(sameHost, connectionFactoryForDefaultVH.getHost());
        assertEquals(5673, connectionFactoryForDefaultVH.getPort());
        assertEquals(sameUsername, connectionFactoryForDefaultVH.getUsername());

        assertEquals(sameVH, connectionFactoryForAnotherVH.getVirtualHost());
        assertEquals(sameHost, connectionFactoryForAnotherVH.getHost());
        assertEquals(5672, connectionFactoryForAnotherVH.getPort());
        assertEquals(sameUsername, connectionFactoryForAnotherVH.getUsername());

        assertEquals(2, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(2, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(2, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(2, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }

    @Test
    void should_create_2_connectionFactories_and_all_other_beans_for_different_hosts_same_port_and_same_virtual_host() {

        String sameVH = "test";
        String sameUsername = "guest";
        String diffHost = "anotherHost";

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queuePropertiesSomeEvent = createQueueProperties(true, sameVH, sameUsername, false, "localhost", 5672);
        rabbitCustomPropertiesMap.put("some-event", queuePropertiesSomeEvent);

        TunedRabbitProperties queuePropertiesAnotherEvent = createQueueProperties(false, sameVH, sameUsername, false, diffHost, 5672);
        rabbitCustomPropertiesMap.put("some-event2", queuePropertiesAnotherEvent);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactoryForDefaultVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesSomeEvent));
        CachingConnectionFactory connectionFactoryForAnotherVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesAnotherEvent));

        assertNotNull(connectionFactoryForDefaultVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesSomeEvent)));

        assertNotNull(connectionFactoryForAnotherVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesAnotherEvent)));

        assertEquals(sameVH, connectionFactoryForDefaultVH.getVirtualHost());
        assertEquals("localhost", connectionFactoryForDefaultVH.getHost());
        assertEquals(5672, connectionFactoryForDefaultVH.getPort());
        assertEquals(sameUsername, connectionFactoryForDefaultVH.getUsername());

        assertEquals(sameVH, connectionFactoryForAnotherVH.getVirtualHost());
        assertEquals(diffHost, connectionFactoryForAnotherVH.getHost());
        assertEquals(5672, connectionFactoryForAnotherVH.getPort());
        assertEquals(sameUsername, connectionFactoryForAnotherVH.getUsername());

        assertEquals(2, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(2, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(2, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(2, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }

    @Test
    void should_create_2_connectionFactories_and_all_other_beans_for_same_host_same_port_and_different_virtual_host() {

        String anotherVirtualHost = "test";
        String anotherUsername = "anotherUsername";

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();

        TunedRabbitProperties queuePropertiesSomeEvent = createQueueProperties(true);
        rabbitCustomPropertiesMap.put("some-event", queuePropertiesSomeEvent);

        TunedRabbitProperties queuePropertiesAnotherEvent = createQueueProperties(false, anotherVirtualHost, anotherUsername, false);
        rabbitCustomPropertiesMap.put("some-event2", queuePropertiesAnotherEvent);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactoryForDefaultVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent));
        CachingConnectionFactory connectionFactoryForAnotherVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesAnotherEvent));

        assertNotNull(connectionFactoryForDefaultVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queuePropertiesSomeEvent)));

        assertNotNull(connectionFactoryForAnotherVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesAnotherEvent)));

        assertEquals("/", connectionFactoryForDefaultVH.getVirtualHost());
        assertEquals("localhost", connectionFactoryForDefaultVH.getHost());
        assertEquals(5672, connectionFactoryForDefaultVH.getPort());
        assertEquals("guest", connectionFactoryForDefaultVH.getUsername());

        assertEquals(anotherVirtualHost, connectionFactoryForAnotherVH.getVirtualHost());
        assertEquals("localhost", connectionFactoryForAnotherVH.getHost());
        assertEquals(5672, connectionFactoryForAnotherVH.getPort());
        assertEquals(anotherUsername, connectionFactoryForAnotherVH.getUsername());

        assertEquals(2, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(2, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(2, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(2, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }

    @Test
    void should_create_2_connectionFactories_and_all_other_beans_for_different_hosts_different_ports_and_same_virtual_host() {

        String sameVH = "test";
        String sameUsername = "guest";
        String anotherHost = "anotherHost";
        int anotherPort = 5673;

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();

        TunedRabbitProperties queuePropertiesSomeEvent = createQueueProperties(false, sameVH, sameUsername, false, "localhost", 5672);
        rabbitCustomPropertiesMap.put("some-event", queuePropertiesSomeEvent);

        TunedRabbitProperties queuePropertiesAnotherEvent = createQueueProperties(false, sameVH, sameUsername, false, anotherHost, anotherPort);
        rabbitCustomPropertiesMap.put("some-event2", queuePropertiesAnotherEvent);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactoryForDefaultVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesSomeEvent));
        CachingConnectionFactory connectionFactoryForAnotherVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesAnotherEvent));

        assertNotNull(connectionFactoryForDefaultVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesSomeEvent)));

        assertNotNull(connectionFactoryForAnotherVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesAnotherEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesAnotherEvent)));

        assertEquals(sameVH, connectionFactoryForDefaultVH.getVirtualHost());
        assertEquals("localhost", connectionFactoryForDefaultVH.getHost());
        assertEquals(5672, connectionFactoryForDefaultVH.getPort());
        assertEquals("guest", connectionFactoryForDefaultVH.getUsername());

        assertEquals(sameVH, connectionFactoryForAnotherVH.getVirtualHost());
        assertEquals(anotherHost, connectionFactoryForAnotherVH.getHost());
        assertEquals(anotherPort, connectionFactoryForAnotherVH.getPort());
        assertEquals(sameUsername, connectionFactoryForAnotherVH.getUsername());

        assertEquals(2, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(2, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(2, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(2, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }

    @Test
    void should_create_only_one_connectionFactory_and_all_other_beans_for_same_host_same_port_and_same_virtual_host() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queuePropertiesSomeEvent = createQueueProperties(true);
        rabbitCustomPropertiesMap.put("some-event", queuePropertiesSomeEvent);

        TunedRabbitProperties queuePropertiesAnotherEvent = createQueueProperties(false);
        rabbitCustomPropertiesMap.put("some-event2", queuePropertiesAnotherEvent);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        CachingConnectionFactory connectionFactoryForDefaultVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent));

        assertNotNull(connectionFactoryForDefaultVH);
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queuePropertiesSomeEvent)));
        assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queuePropertiesSomeEvent)));

        assertEquals("/", connectionFactoryForDefaultVH.getVirtualHost());
        assertEquals("localhost", connectionFactoryForDefaultVH.getHost());
        assertEquals(5672, connectionFactoryForDefaultVH.getPort());
        assertEquals("guest", connectionFactoryForDefaultVH.getUsername());

        assertEquals(1, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(1, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(1, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(1, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }

    @Test
    void should_create_create_connectionFactory_without_host_and_port_when_cluster_mode() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        TunedRabbitProperties queueProperties = createQueueProperties(true);
        queueProperties.setClusterMode(true);
        queueProperties.setHosts("127.0.0.1:5672,127.0.0.1:6672");
        // set this to assert that wasn't used
        queueProperties.setHost("tradeshift");
        queueProperties.setPort(6672);
        // spying to assert that, when in cluster mode, the get hosts is called
        TunedRabbitProperties spyQueueProperties = spy(queueProperties);
        rabbitCustomPropertiesMap.put("some-event", spyQueueProperties);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);
        
        // verify if it was called at least once for each component:
        // connection factory, rabbit template, rabbit admin and listener container
        verify(spyQueueProperties, atLeast(4)).isClusterMode();
        verify(spyQueueProperties, atLeast(4)).getHosts();
        verify(spyQueueProperties, never()).getHost();

        CachingConnectionFactory connectionFactory = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanNameForDefaultVirtualHost(queueProperties));
        RabbitTemplate rabbitTemplate = (RabbitTemplate) context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanNameForDefaultVirtualHost(queueProperties));
        RabbitAdmin rabbitAdmin = (RabbitAdmin) context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanNameForDefaultVirtualHost(queueProperties));
        SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory =
                (SimpleRabbitListenerContainerFactory) context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBeanForDefaultVirtualHost(queueProperties));

        assertNotNull(connectionFactory);
        assertNotNull(rabbitTemplate);
        assertNotNull(rabbitAdmin);
        assertNotNull(simpleRabbitListenerContainerFactory);

        // default values from Spring
        assertEquals(null, connectionFactory.getHost());
        assertEquals(0, connectionFactory.getPort());
        assertEquals("/", connectionFactory.getVirtualHost());
        assertEquals("guest", connectionFactory.getUsername());
        
        // assure that have hosts
        Field addressesField = AbstractConnectionFactory.class.getDeclaredField("addresses");
        addressesField.setAccessible(true);
        List<Address> addresses = (List<Address>) addressesField.get(connectionFactory);
        List<String> hosts = addresses.stream()
                .map(a -> String.format("%s:%d", a.getHost(), a.getPort()))
                .collect(toList());
        
        assertTrue(hosts.containsAll(List.of("127.0.0.1:5672", "127.0.0.1:6672")));

        assertEquals(1, context.getBeansOfType(CachingConnectionFactory.class).size());
        assertEquals(1, context.getBeansOfType(RabbitTemplate.class).size());
        assertEquals(1, context.getBeansOfType(RabbitAdmin.class).size());
        assertEquals(1, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }

    @Test
    void should_create_ssl_connectionFactory_and_all_other_beans_for_same_host_same_port_and_same_virtual_host() {

      String sameVH = "test";
      String sameUsername = "guest";
      String sameHost = "localhost";

      TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
      TunedRabbitProperties queuePropertiesSomeEvent = createQueueProperties(true, sameVH, sameUsername, false, sameHost, 5671);
      rabbitCustomPropertiesMap.put("some-event", queuePropertiesSomeEvent);

      tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

      CachingConnectionFactory connectionFactoryForDefaultVH = (CachingConnectionFactory) context.getBean(RabbitBeanNameResolver.getConnectionFactoryBeanName(queuePropertiesSomeEvent));

      assertNotNull(connectionFactoryForDefaultVH);
      assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitTemplateBeanName(queuePropertiesSomeEvent)));
      assertNotNull(context.getBean(RabbitBeanNameResolver.getRabbitAdminBeanName(queuePropertiesSomeEvent)));
      assertNotNull(context.getBean(RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(queuePropertiesSomeEvent)));

      assertEquals(sameVH, connectionFactoryForDefaultVH.getVirtualHost());
      assertEquals(null, connectionFactoryForDefaultVH.getHost());
      assertEquals(0, connectionFactoryForDefaultVH.getPort());
      assertEquals(sameUsername, connectionFactoryForDefaultVH.getUsername());

      assertEquals(1, context.getBeansOfType(CachingConnectionFactory.class).size());
      assertEquals(1, context.getBeansOfType(RabbitTemplate.class).size());
      assertEquals(1, context.getBeansOfType(RabbitAdmin.class).size());
      assertEquals(1, context.getBeansOfType(SimpleRabbitListenerContainerFactory.class).size());
    }
    
    private TunedRabbitProperties createQueueProperties(boolean primary) {
        return createQueueProperties(primary, null);
    }

    private TunedRabbitProperties createQueueProperties(boolean primary, String virtualHost) {
        return createQueueProperties(primary, virtualHost, false);
    }

    private TunedRabbitProperties createQueueProperties(boolean primary, String virtualHost, boolean jsonMessageConverter) {
        return createQueueProperties(primary, virtualHost, "guest", jsonMessageConverter);
    }

    private TunedRabbitProperties createQueueProperties(boolean primary, String virtualHost, String username, boolean jsonMessageConverter) {
        return createQueueProperties(primary, virtualHost, username, jsonMessageConverter, "localhost", 5672);
    }

    private TunedRabbitProperties createQueueProperties(boolean primary, String virtualHost, String username, boolean jsonMessageConverter, String host, int port) {
        TunedRabbitProperties queueProperties = new TunedRabbitProperties();
        queueProperties.setQueue("queue.test");
        queueProperties.setExchange("ex.test");
        queueProperties.setExchangeType("topic");
        queueProperties.setMaxRetriesAttempts(5);
        queueProperties.setQueueRoutingKey("routing.key.test");
        queueProperties.setTtlRetryMessage(3000);
        queueProperties.setPrimary(primary);
        queueProperties.setVirtualHost(virtualHost);
        queueProperties.setUsername(username);
        queueProperties.setPassword("guest");
        queueProperties.setHost(host);
        queueProperties.setPort(port);
        queueProperties.setSslConnection(false);
        queueProperties.setEnableJsonMessageConverter(jsonMessageConverter);

        return queueProperties;
    }

    private AbstractMessageConverter getMessageConverter(SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory) throws IllegalAccessException {
        Field[] fields = simpleRabbitListenerContainerFactory.getClass().getSuperclass().getDeclaredFields();
        for (Field field : fields) {
            if ("messageConverter".equals(field.getName())) {
                field.setAccessible(true);
                return (AbstractMessageConverter) field.get(simpleRabbitListenerContainerFactory);
            }
        }

        return null;
    }

}
