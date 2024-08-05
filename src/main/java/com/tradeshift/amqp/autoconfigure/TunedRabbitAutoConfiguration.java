package com.tradeshift.amqp.autoconfigure;

import com.rabbitmq.client.Channel;
import com.tradeshift.amqp.annotation.EnableRabbitRetryAfterThrowTunedRabbitExceptionAspect;
import com.tradeshift.amqp.annotation.EnableRabbitRetryAndDlqAspect;
import com.tradeshift.amqp.log.TunedRabbitConstants;
import com.tradeshift.amqp.rabbit.annotation.TunedRabbitListenerAnnotationBeanPostProcessor;
import com.tradeshift.amqp.rabbit.components.QueueFactory;
import com.tradeshift.amqp.rabbit.components.RabbitComponentsFactory;
import com.tradeshift.amqp.rabbit.handlers.RabbitAdminHandler;
import com.tradeshift.amqp.rabbit.handlers.RabbitTemplateHandler;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesBindHandlerAdvisor;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;
import com.tradeshift.amqp.rabbit.retry.QueueRetryComponent;
import com.tradeshift.amqp.resolvers.RabbitBeanNameResolver;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListenerAnnotationBeanPostProcessor;
import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@EnableConfigurationProperties(TunedRabbitPropertiesMap.class)
@Configuration
@ConditionalOnClass({RabbitTemplate.class, Channel.class})
@AutoConfigureBefore(RabbitAutoConfiguration.class)
@Import({TunedRabbitAutoConfiguration.RabbitPostProcessorConfiguration.class, TunedRabbitPropertiesBindHandlerAdvisor.class })
public class TunedRabbitAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(TunedRabbitAutoConfiguration.class);
    private Set<String> virtualHosts = new HashSet<>();
    private Set<String> portAndHost = new HashSet<>();

    private final ApplicationContext applicationContext;
    private final ConfigurableListableBeanFactory beanFactory;

    @Autowired
    public TunedRabbitAutoConfiguration(ApplicationContext applicationContext, ConfigurableListableBeanFactory beanFactory) {
        this.applicationContext = applicationContext;
        this.beanFactory = beanFactory;
    }

    @ConditionalOnProperty(
            value = "spring.rabbitmq.enable.custom.autoconfiguration",
            havingValue = "true",
            matchIfMissing = true)
    @Configuration
    static class RabbitPostProcessorConfiguration {
        @Bean(name = RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
        @DependsOn(TunedRabbitConstants.CONNECTION_FACTORY_BEAN_NAME)
        public static RabbitListenerAnnotationBeanPostProcessor rabbitListenerAnnotationProcessor() {
            return new TunedRabbitListenerAnnotationBeanPostProcessor();
        }
    }

    @ConditionalOnProperty(
            value = "spring.rabbitmq.enable.custom.autoconfiguration",
            havingValue = "true",
            matchIfMissing = true)
    @Configuration
    @EnableRabbit
    static class EnableRabbitConfiguration {
        @Bean(name = RabbitListenerConfigUtils.RABBIT_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
        public static RabbitListenerAnnotationBeanPostProcessor rabbitListenerAnnotationProcessor() {
            return new TunedRabbitListenerAnnotationBeanPostProcessor();
        }
    }

    @Bean
    @ConditionalOnProperty(
            value = "spring.rabbitmq.enable.custom.autoconfiguration",
            havingValue = "true",
            matchIfMissing = true)
    @DependsOn(TunedRabbitConstants.CONNECTION_FACTORY_BEAN_NAME)
    public RabbitTemplateHandler rabbitTemplateHandler(TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        return new RabbitTemplateHandler(applicationContext, rabbitCustomPropertiesMap);
    }

    @Bean
    @ConditionalOnProperty(
            value = "spring.rabbitmq.enable.custom.autoconfiguration",
            havingValue = "true",
            matchIfMissing = true)
    @DependsOn(TunedRabbitConstants.CONNECTION_FACTORY_BEAN_NAME)
    public RabbitAdminHandler rabbitAdminHandler(TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        return new RabbitAdminHandler(applicationContext, rabbitCustomPropertiesMap);
    }

    @Bean("rabbitTemplateHandler")
    @ConditionalOnProperty(
            value = "spring.rabbitmq.enable.custom.autoconfiguration",
            havingValue = "false")
    public RabbitTemplateHandler rabbitTemplateHandlerWithoutAutoConfig(TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        return new RabbitTemplateHandler(applicationContext, rabbitCustomPropertiesMap);
    }

    @Bean("rabbitAdminHandler")
    @ConditionalOnProperty(
            value = "spring.rabbitmq.enable.custom.autoconfiguration",
            havingValue = "false")
    public RabbitAdminHandler rabbitAdminHandlerWithoutAutoConfig(TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        return new RabbitAdminHandler(applicationContext, rabbitCustomPropertiesMap);
    }

    @Bean
    @DependsOn("rabbitTemplateHandler")
    public QueueRetryComponent queueRetryComponent(TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        return new QueueRetryComponent(rabbitTemplateHandler(rabbitCustomPropertiesMap));
    }

    @Bean
    @DependsOn("queueRetryComponent")
    public EnableRabbitRetryAndDlqAspect enableRabbitRetryAndDlqAspect(TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        return new EnableRabbitRetryAndDlqAspect(queueRetryComponent(rabbitCustomPropertiesMap), rabbitCustomPropertiesMap);
    }
    
    @Bean
    @DependsOn("queueRetryComponent")
    public EnableRabbitRetryAfterThrowTunedRabbitExceptionAspect enableRabbitRetryAfterThrowTunnedRabbitExceptionAspect(TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
    	return new EnableRabbitRetryAfterThrowTunedRabbitExceptionAspect(queueRetryComponent(rabbitCustomPropertiesMap), rabbitCustomPropertiesMap);
    }

    @Bean
    public RabbitComponentsFactory rabbitComponentsFactory() {
    	return new RabbitComponentsFactory();
    }

    @ConditionalOnProperty(
            value = "spring.rabbitmq.enable.custom.autoconfiguration",
            havingValue = "true",
            matchIfMissing = true)
    @Bean
    @DependsOn("rabbitComponentsFactory")
    public MessageConverter producerJackson2MessageConverter() {
        return rabbitComponentsFactory().createJackson2MessageConverter();
    }

    @ConditionalOnProperty(
            value = "spring.rabbitmq.enable.custom.autoconfiguration",
            havingValue = "true",
            matchIfMissing = true)
    @Primary
    @Bean(TunedRabbitConstants.CONNECTION_FACTORY_BEAN_NAME)
    @DependsOn({ "producerJackson2MessageConverter", "rabbitComponentsFactory" })
    public ConnectionFactory routingConnectionFactory(TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        validateSinglePrimaryConnection(rabbitCustomPropertiesMap);

        AtomicReference<ConnectionFactory> defaultConnectionFactory = new AtomicReference<>();

        HashMap<Object, ConnectionFactory> connectionFactoryHashMap = new HashMap<>();
        rabbitCustomPropertiesMap.forEach((eventName, properties) -> {
            properties.setEventName(eventName);
            ConnectionFactory connectionFactory = createRabbitMQArch(properties, connectionFactoryHashMap);

            connectionFactoryHashMap.put(
                    RabbitBeanNameResolver.getConnectionFactoryBeanName(properties),
                    connectionFactory
            );

            if (properties.isPrimary()) {
                defaultConnectionFactory.set(connectionFactory);
            }
        });

        if (Objects.isNull(defaultConnectionFactory.get())) {
            Optional<ConnectionFactory> first = connectionFactoryHashMap.values().stream().findFirst();
            first.ifPresent(defaultConnectionFactory::set);
        }

        return rabbitComponentsFactory().createSimpleRoutingConnectionFactory(defaultConnectionFactory, connectionFactoryHashMap);
    }

    private void validateSinglePrimaryConnection(TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        long primary = rabbitCustomPropertiesMap.stream()
                .filter(stringQueuePropertiesEntry -> stringQueuePropertiesEntry.getValue().isPrimary())
                .count();

        if (primary > 1) {
            throw new IllegalArgumentException("Only one primary RabbitMQ architecture is allowed!");
        }
    }

    private ConnectionFactory createRabbitMQArch(final TunedRabbitProperties property, HashMap<Object, ConnectionFactory> connectionFactoryHashMap) {
        final String virtualHost = RabbitBeanNameResolver.treatVirtualHostName(property.getVirtualHost());

        if (!portAndHost.contains(getTunedRabbitPropertiesCacheKey(property))) {
            applyAutoConfiguration(property);
        } else if (!virtualHosts.contains(virtualHost)) {
            applyAutoConfiguration(property);
        } else {
        	applyAutoConfigurationOnlyForBinding(property, connectionFactoryHashMap.get(RabbitBeanNameResolver.getConnectionFactoryBeanName(property)));
        }

        return (CachingConnectionFactory) applicationContext.getBean(RabbitBeanNameResolver
                .getConnectionFactoryBeanName(property));
    }

    private void applyAutoConfiguration(final TunedRabbitProperties property) {
    	final RabbitComponentsFactory rabbitComponentsFactory = rabbitComponentsFactory();

    	final String virtualHost = RabbitBeanNameResolver.treatVirtualHostName(property.getVirtualHost());
    	CachingConnectionFactory connectionsFactoryBean = rabbitComponentsFactory.createCachingConnectionFactory(property, virtualHost);

        Optional.ofNullable(connectionsFactoryBean).ifPresent(connectionFactory -> {
            String connectionFactoryBeanName = RabbitBeanNameResolver.getConnectionFactoryBeanName(virtualHost, property);
            beanFactory.registerSingleton(connectionFactoryBeanName, connectionFactory);
            log.info("ConnectionFactory Bean with name {} was created for the event {} and virtual host {}",
                    connectionFactoryBeanName, property.getEventName(), virtualHost);

        	String listenerContainerFactoryBeanName = RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(virtualHost, property);
        	SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactoryBeanDef = rabbitComponentsFactory.createSimpleRabbitListenerContainerFactoryBean(property, connectionFactory);
        	beanFactory.registerSingleton(listenerContainerFactoryBeanName, simpleRabbitListenerContainerFactoryBeanDef);
        	log.info("SimpleRabbitListenerContainerFactory Bean with name {} was created for the event {} and virtual host {}",
        			listenerContainerFactoryBeanName, property.getEventName(), virtualHost);

            RabbitAdmin beanDefinitionRabbitAdmin = rabbitComponentsFactory.createRabbitAdminBean(connectionFactory);
            String rabbitAdminBeanName = RabbitBeanNameResolver.getRabbitAdminBeanName(virtualHost, property);
            beanFactory.registerSingleton(rabbitAdminBeanName, beanDefinitionRabbitAdmin);
            log.info("RabbitAdmin Bean with name {} was created for the event {} and virtual host {}",
                    rabbitAdminBeanName, property.getEventName(), virtualHost);

            RabbitTemplate beanDefinitionRabbitTemplate = rabbitComponentsFactory.createRabbitTemplateBean(connectionFactory, property);
            String rabbitTemplateBeanName = RabbitBeanNameResolver.getRabbitTemplateBeanName(virtualHost, property);
            beanFactory.registerSingleton(rabbitTemplateBeanName, beanDefinitionRabbitTemplate);
            log.info("RabbitTemplate Bean with name {} was created for the event {} and virtual host {}",
                    rabbitTemplateBeanName, property.getEventName(), virtualHost);

            virtualHosts.add(virtualHost);
            portAndHost.add(getTunedRabbitPropertiesCacheKey(property));

            if (property.isAutoCreate() || (property.isAutoCreateOnlyForTest() && isTestProfile())) {
                autoCreateQueues(property, beanDefinitionRabbitAdmin);
            }
        });
    }

    /**
     * Apply the auto configuration to create the binding between exchange and queue.
     * It tries to recover the ConnectionFactory and RabbitAdmin from bean factory.
     */
    private void applyAutoConfigurationOnlyForBinding(final TunedRabbitProperties properties, ConnectionFactory connectionFactory) {
    	final String virtualHost = RabbitBeanNameResolver.treatVirtualHostName(properties.getVirtualHost());

    	String rabbitAdminBeanName = RabbitBeanNameResolver.getRabbitAdminBeanName(virtualHost, properties);
    	log.info("Getting RabbitAdmin Bean with name {} for the event {} and virtual host {}",
                rabbitAdminBeanName, properties.getEventName(), virtualHost);
    	RabbitAdmin rabbitAdmin = beanFactory.getBean(rabbitAdminBeanName, RabbitAdmin.class);
    	
    	String listenerContainerFactoryBeanName = RabbitBeanNameResolver.getSimpleRabbitListenerContainerFactoryBean(virtualHost, properties);
    	if(!beanFactory.containsBean(listenerContainerFactoryBeanName)) {
    		final RabbitComponentsFactory rabbitComponentsFactory = rabbitComponentsFactory();
    		SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactoryBeanDef = rabbitComponentsFactory.createSimpleRabbitListenerContainerFactoryBean(properties, (CachingConnectionFactory) connectionFactory);
    		beanFactory.registerSingleton(listenerContainerFactoryBeanName, simpleRabbitListenerContainerFactoryBeanDef);
    		log.info("SimpleRabbitListenerContainerFactory Bean with name {} was created for the event {} and virtual host {}",
    				listenerContainerFactoryBeanName, properties.getEventName(), virtualHost);
    	}
    	

        if (properties.isAutoCreate() || (properties.isAutoCreateOnlyForTest() && isTestProfile())) {
            autoCreateQueues(properties, rabbitAdmin);
        }
    }

    private void autoCreateQueues(TunedRabbitProperties properties, RabbitAdmin rabbitAdmin) {
        QueueFactory queueFactory = new QueueFactory(properties, rabbitAdmin);
        queueFactory.create();
    }

    private String getTunedRabbitPropertiesCacheKey(TunedRabbitProperties properties) {
        if (properties.isClusterMode()) {
            // TODO: maybe improve so that the order of each host:port in the list doesn't generate different key
            return properties.getHosts();
        }
        return properties.getPort() + properties.getHost();
    }

    private boolean isTestProfile() {
        return Arrays.asList(applicationContext.getEnvironment().getActiveProfiles()).contains("test");
    }

}
