package com.tradeshift.amqp.rabbit.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.tradeshift.amqp.autoconfigure.TunedRabbitAutoConfiguration;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;

@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RabbitTemplateHandlerTest {

    private TunedRabbitAutoConfiguration tradeshiftRabbitAutoConfiguration;

    @Spy
    private AnnotationConfigApplicationContext spyContext;

    @Autowired
    private GenericApplicationContext context;

    @Autowired
    private ConfigurableListableBeanFactory beanFactory;
    
    private RabbitTemplateHandler rabbitTemplateHandler;

    @BeforeEach
    void setup() {
        tradeshiftRabbitAutoConfiguration = new TunedRabbitAutoConfiguration(context, beanFactory);
    }

    @Test
    void should_return_default_rabbit_template() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, null));

        rabbitTemplateHandler = new RabbitTemplateHandler(context, rabbitCustomPropertiesMap);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        assertNotNull(rabbitTemplateHandler.getRabbitTemplate("some-event"));
    }

    @Test
    void should_return_no_such_bean_definition_exception() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, "test"));

        rabbitTemplateHandler = new RabbitTemplateHandler(context, rabbitCustomPropertiesMap);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);
        
        NoSuchBeanDefinitionException ex = assertThrows(NoSuchBeanDefinitionException.class, () -> rabbitTemplateHandler.getRabbitTemplate("test"));
        assertEquals("No bean named 'test' available", ex.getMessage());
    }

    @Test
    void should_return_all_rabbit_templates() {

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, null));
        rabbitCustomPropertiesMap.put("some-event2", createQueueProperties(false, "test"));

        rabbitTemplateHandler = new RabbitTemplateHandler(context, rabbitCustomPropertiesMap);

        tradeshiftRabbitAutoConfiguration.routingConnectionFactory(rabbitCustomPropertiesMap);

        assertNotNull(rabbitTemplateHandler.getRabbitTemplate("some-event"));
        assertNotNull(rabbitTemplateHandler.getRabbitTemplate("some-event2"));
    }

    @Test
    void should_return_exception_when_we_have_more_than_2_templates_with_autoconfig_disabled_and_without_the_correct_properties() {

        disableAutoConfigurationInSpyContext();
        Map<String, RabbitTemplate> beansOfType = new HashMap<>();
        beansOfType.put("rt1", new RabbitTemplate());
        beansOfType.put("rt2", new RabbitTemplate());

        Mockito.doReturn(new RabbitTemplate()).when(spyContext).getBean(Mockito.anyString());
        Mockito.doReturn(beansOfType).when(spyContext).getBeansOfType(RabbitTemplate.class);

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, null));
        rabbitCustomPropertiesMap.put("some-event2", createQueueProperties(false, "test"));

        rabbitTemplateHandler = new RabbitTemplateHandler(spyContext, rabbitCustomPropertiesMap);
        
        BeanDefinitionValidationException ex1 = assertThrows(BeanDefinitionValidationException.class, () ->  rabbitTemplateHandler.getRabbitTemplate("some-event"));
        assertEquals("There are more than 1 RabbitTemplate available. You need to specify the name of the RabbitTemplate that we will use for this event", ex1.getMessage());
        
        BeanDefinitionValidationException ex2 = assertThrows(BeanDefinitionValidationException.class, () ->  rabbitTemplateHandler.getRabbitTemplate("some-event2"));
        assertEquals("There are more than 1 RabbitTemplate available. You need to specify the name of the RabbitTemplate that we will use for this event", ex2.getMessage());
    }

    @Test
    void should_return_the_single_bean_when_we_have_the_autoconfig_disabled_and_without_the_correct_properties() {
        disableAutoConfigurationInSpyContext();
        Map<String, RabbitTemplate> beansOfType = new HashMap<>();
        beansOfType.put("rt1", new RabbitTemplate());

        Mockito.doReturn(new RabbitTemplate()).when(spyContext).getBean(Mockito.anyString());
        Mockito.doReturn(beansOfType).when(spyContext).getBeansOfType(RabbitTemplate.class);

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, null));

        rabbitTemplateHandler = new RabbitTemplateHandler(spyContext, rabbitCustomPropertiesMap);

        assertNotNull(rabbitTemplateHandler.getRabbitTemplate("some-event"));
    }

    @Test
    void should_return_exception_when_we_have_the_autoconfig_disabled_and_without_any_rabbit_template_bean_configured() {

        disableAutoConfigurationInSpyContext();

        Mockito.doReturn(new RabbitTemplate()).when(spyContext).getBean(Mockito.anyString());
        Mockito.doReturn(new HashMap<>()).when(spyContext).getBeansOfType(RabbitTemplate.class);

        Mockito.doReturn(new RabbitTemplate()).when(spyContext).getBean(Mockito.anyString());

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, null));

        rabbitTemplateHandler = new RabbitTemplateHandler(spyContext, rabbitCustomPropertiesMap);

        NoSuchBeanDefinitionException ex = assertThrows(NoSuchBeanDefinitionException.class, () ->  rabbitTemplateHandler.getRabbitTemplate("some-event"));
        assertEquals("No bean named 'null' available: No RabbitTemplate bean available. Are you sure that you want to disable the autoconfiguration?", ex.getMessage());
        
    }

    @Test
    void should_return_the_correct_rabbit_template_bean_when_we_have_more_than_2_templates_with_autoconfig_disabled_and_with_the_correct_properties() {
        disableAutoConfigurationInSpyContext();

        String rabbitTemplateBean1 = "rt1";
        String rabbitTemplateBean2 = "rt2";

        Map<String, RabbitTemplate> beansOfType = new HashMap<>();
        beansOfType.put(rabbitTemplateBean1, new RabbitTemplate());
        beansOfType.put(rabbitTemplateBean2, new RabbitTemplate());

        Mockito.doReturn(new RabbitTemplate()).when(spyContext).getBean(rabbitTemplateBean1);
        Mockito.doReturn(new RabbitTemplate()).when(spyContext).getBean(rabbitTemplateBean2);
        Mockito.doReturn(beansOfType).when(spyContext).getBeansOfType(RabbitTemplate.class);

        TunedRabbitPropertiesMap rabbitCustomPropertiesMap = new TunedRabbitPropertiesMap();
        rabbitCustomPropertiesMap.put("some-event", createQueueProperties(true, null, rabbitTemplateBean1));
        rabbitCustomPropertiesMap.put("some-event2", createQueueProperties(false, "test", rabbitTemplateBean2));

        rabbitTemplateHandler = new RabbitTemplateHandler(spyContext, rabbitCustomPropertiesMap);

        assertNotNull(rabbitTemplateHandler.getRabbitTemplate("some-event"));
        assertNotNull(rabbitTemplateHandler.getRabbitTemplate("some-event2"));
    }

    private TunedRabbitProperties createQueueProperties(boolean primary, String virtualHost, String rabbitTemplateName) {
        return createQueueProperties(primary, virtualHost, "guest", "localhost", 5672, rabbitTemplateName);
    }

    private TunedRabbitProperties createQueueProperties(boolean primary, String virtualHost) {
        return createQueueProperties(primary, virtualHost, "guest", "localhost", 5672, null);
    }

    private void disableAutoConfigurationInSpyContext(){
        MockEnvironment mockEnvironment = new MockEnvironment();
        mockEnvironment.setProperty("spring.rabbitmq.enable.custom.autoconfiguration", "false");
        spyContext.setEnvironment(mockEnvironment);
    }

    private TunedRabbitProperties createQueueProperties(boolean primary, String virtualHost, String username, String host, int port, String rabbitTemplateName) {
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
        Optional.ofNullable(rabbitTemplateName).ifPresent(queueProperties::setRabbitTemplateBeanName);

        return queueProperties;
    }

}
