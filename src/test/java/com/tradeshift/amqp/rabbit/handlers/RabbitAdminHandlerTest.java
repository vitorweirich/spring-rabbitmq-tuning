package com.tradeshift.amqp.rabbit.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.tradeshift.amqp.autoconfigure.TunedRabbitAutoConfiguration;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;

@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RabbitAdminHandlerTest {

    private TunedRabbitAutoConfiguration tradeshiftRabbitAutoConfiguration;

    @Autowired
    private GenericApplicationContext context;

    @Autowired
    private ConfigurableListableBeanFactory beanFactory;
    
    private RabbitAdminHandler rabbitAdminHandler;

    @BeforeEach
    void setup() {
        tradeshiftRabbitAutoConfiguration = new TunedRabbitAutoConfiguration(context, beanFactory);
    }

    @Test
    void should_return_default_rabbit_template() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, null));

        rabbitAdminHandler = new RabbitAdminHandler(context, rabbitCustomPropertiesMap);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        assertNotNull(rabbitAdminHandler.getRabbitAdmin("some-event"));
    }

    @Test
    void should_return_rabbit_template_by_host_and_port_and_virtual_host() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, null));

        rabbitAdminHandler = new RabbitAdminHandler(context, rabbitCustomPropertiesMap);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        assertNotNull(rabbitAdminHandler.getRabbitAdminByHostAndPortAndVirtualHost("DefaultLocalhost5672"));
    }

    @Test
    void should_return_no_such_bean_definition_exception_for_another_virtual_host() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, "vh"));

        rabbitAdminHandler = new RabbitAdminHandler(context, rabbitCustomPropertiesMap);

        NoSuchBeanDefinitionException ex = assertThrows(NoSuchBeanDefinitionException.class, () -> rabbitAdminHandler.getRabbitAdmin("test"));
        assertEquals("No bean named 'test' available", ex.getMessage());
    }

    @Test
    void should_return_all_rabbit_templates() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, null));
        rabbitCustomPropertiesMap.put("some-event2", createQueueProperties(false, "test"));

        rabbitAdminHandler = new RabbitAdminHandler(context, rabbitCustomPropertiesMap);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        assertNotNull(rabbitAdminHandler.getRabbitAdmin("some-event"));
        assertNotNull(rabbitAdminHandler.getRabbitAdmin("some-event2"));
    }

    private TunedRabbitProperties createQueueProperties(boolean primary, String virtualHost) {
        return createQueueProperties(primary, virtualHost, "guest", "localhost", 5672);
    }

    private TunedRabbitProperties createQueueProperties(boolean primary, String virtualHost, String username, String host, int port) {
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
        queueProperties.setSslConnection(false);
        queueProperties.setHost(host);
        queueProperties.setPort(port);

        return queueProperties;
    }

}
