package com.tradeshift.amqp.rabbit.components;

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;

import com.rabbitmq.client.DefaultSaslConfig;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;

public class RabbitComponentsFactory {
	private static final Logger log = LoggerFactory.getLogger(RabbitComponentsFactory.class);

	public RabbitAdmin createRabbitAdminBean(CachingConnectionFactory connectionFactory) {
		return new RabbitAdmin(connectionFactory);
	}

	public MessageConverter createJackson2MessageConverter() {
		return new Jackson2JsonMessageConverter();
	}

	public RabbitTemplate createRabbitTemplateBean(CachingConnectionFactory connectionFactory, final TunedRabbitProperties property) {
		RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
		if (property.isEnableJsonMessageConverter()) {
			rabbitTemplate.setMessageConverter(createJackson2MessageConverter());
		}
		return rabbitTemplate;
	}

	public ConnectionFactory createSimpleRoutingConnectionFactory(AtomicReference<ConnectionFactory> defaultConnectionFactory,
			HashMap<Object, ConnectionFactory> connectionFactoryHashMap) {
		SimpleRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();
		connectionFactory.setTargetConnectionFactories(connectionFactoryHashMap);
		connectionFactory.setDefaultTargetConnectionFactory(defaultConnectionFactory.get());
		return connectionFactory;
	}

	public CachingConnectionFactory createCachingConnectionFactory(final TunedRabbitProperties property, String virtualHost) {
		try {
			com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
			if (property.getPort() == 5671) {
				property.setSslConnection(true);
		    }

			if (!property.isSslConnection()) {
				factory.setUsername(property.getUsername());
				factory.setPassword(property.getPassword());
			} else {
				factory.setUsername(property.getUsername());
				factory.setPassword(property.getPassword());
				factory.setSaslConfig(DefaultSaslConfig.PLAIN);
				factory.useSslProtocol("TLSv1.2");
				property.setClusterMode(true);
				property.setHosts(property.getHost());
			}
			factory.setAutomaticRecoveryEnabled(property.isAutomaticRecovery());
			Optional.ofNullable(property.getVirtualHost()).ifPresent(factory::setVirtualHost);

			if (property.isClusterMode()) {
				log.info("Event {} configured with cluster mode on", property.getEventName());
				factory.setHost(null);
				factory.setPort(0);
				CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(factory);
				cachingConnectionFactory.setAddresses(property.getHosts());
				return cachingConnectionFactory;
			}

			factory.setHost(property.getHost());
			factory.setPort(property.getPort());

			return new CachingConnectionFactory(factory);
		} catch (Exception e) {
			log.error(String.format("It is not possible create a Connection Factory to Virtual Host %s", virtualHost), e);
			return null;
		}
	}

	public SimpleRabbitListenerContainerFactory createSimpleRabbitListenerContainerFactoryBean(
			final TunedRabbitProperties property, CachingConnectionFactory connectionFactory) {
		SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory = new SimpleRabbitListenerContainerFactory();
		simpleRabbitListenerContainerFactory.setConnectionFactory(connectionFactory);
		simpleRabbitListenerContainerFactory.setConcurrentConsumers(property.getConcurrentConsumers());
		simpleRabbitListenerContainerFactory.setMaxConcurrentConsumers(property.getMaxConcurrentConsumers());
		simpleRabbitListenerContainerFactory.setPrefetchCount(property.getPrefetchCount());
		if(property.isBatchListener() && property.isConsumerBatchEnableds()) {
			simpleRabbitListenerContainerFactory.setBatchListener(true);
			simpleRabbitListenerContainerFactory.setConsumerBatchEnabled(true);
			simpleRabbitListenerContainerFactory.setBatchSize(property.getBatchSize());
			simpleRabbitListenerContainerFactory.setReceiveTimeout(property.getReceiveTimeout());
		}
		if (property.isEnableJsonMessageConverter()) {
			simpleRabbitListenerContainerFactory.setMessageConverter(createJackson2MessageConverter());
		} else {
			simpleRabbitListenerContainerFactory.setMessageConverter(new SimpleMessageConverter());
		}
		return simpleRabbitListenerContainerFactory;
	}
}
