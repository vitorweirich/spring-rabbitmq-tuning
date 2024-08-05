package com.tradeshift.amqp.resolvers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;

class RabbitBeanNameResolverTest {

	@Test
	void should_return_the_correct_name_for_default_connection_factory_from_properties() {
		assertEquals("connectionFactoryDefaultLocalhost5672", RabbitBeanNameResolver
				.getConnectionFactoryBeanName(createQueueProperties("localhost", 5672, null)));
	}

	@Test
	void should_return_the_correct_name_for_connection_factory_from_properties() {
		assertEquals("connectionFactoryTradeshiftLocalhost5672", RabbitBeanNameResolver
				.getConnectionFactoryBeanName(createQueueProperties("localhost", 5672, "tradeshift")));
	}

	@Test
	void should_return_the_correct_name_for_default_connection_factory_from_properties_with_cluster_mode() {
		assertEquals("connectionFactoryDefaultLocalhost5672localhost6672", RabbitBeanNameResolver
				.getConnectionFactoryBeanName(createQueuePropertiesInClusterMode("localhost:5672,localhost:6672", null)));
	}

	@Test
	void should_return_the_correct_name_for_connection_factory_from_properties_with_cluster_mode() {
		assertEquals("connectionFactoryTradeshiftLocalhost5672localhost6672", RabbitBeanNameResolver
				.getConnectionFactoryBeanName(createQueuePropertiesInClusterMode("localhost:5672,localhost:6672", "tradeshift")));
	}

	@Test
	void should_return_the_correct_name_for_default_rabbit_admin_from_properties() {
		assertEquals("rabbitAdminDefaultLocalhost5672", RabbitBeanNameResolver
				.getRabbitAdminBeanName(createQueueProperties("localhost", 5672, null)));
	}

	@Test
	void should_return_the_correct_name_for_rabbit_admin_from_properties() {
		assertEquals("rabbitAdminTradeshiftLocalhost5672", RabbitBeanNameResolver
				.getRabbitAdminBeanName(createQueueProperties("localhost", 5672, "tradeshift")));
	}

	@Test
	void should_return_the_correct_name_for_default_rabbit_admin_from_properties_with_cluster_mode() {
		assertEquals("rabbitAdminDefaultLocalhost5672localhost6672", RabbitBeanNameResolver
				.getRabbitAdminBeanName(createQueuePropertiesInClusterMode("localhost:5672,localhost:6672", null)));
	}

	@Test
	void should_return_the_correct_name_for_rabbit_admin_from_properties_with_cluster_mode() {
		assertEquals("rabbitAdminTradeshiftLocalhost5672localhost6672", RabbitBeanNameResolver
				.getRabbitAdminBeanName(createQueuePropertiesInClusterMode("localhost:5672,localhost:6672", "tradeshift")));
	}

	@Test
	void should_return_the_correct_name_for_default_rabbit_template_from_properties() {
		assertEquals("rabbitTemplateDefaultLocalhost5672", RabbitBeanNameResolver
				.getRabbitTemplateBeanName(createQueueProperties("localhost", 5672, null)));
	}

	@Test
	void should_return_the_correct_name_for_rabbit_template_from_properties() {
		assertEquals("rabbitTemplateTradeshiftLocalhost5672", RabbitBeanNameResolver
				.getRabbitTemplateBeanName(createQueueProperties("localhost", 5672, "tradeshift")));
	}

	@Test
	void should_return_the_correct_name_for_default_rabbit_template_from_properties_with_cluster_mode() {
		assertEquals("rabbitTemplateDefaultLocalhost5672localhost6672", RabbitBeanNameResolver
				.getRabbitTemplateBeanName(createQueuePropertiesInClusterMode("localhost:5672,localhost:6672", null)));
	}

	@Test
	void should_return_the_correct_name_for_rabbit_template_from_properties_with_cluster_mode() {
		assertEquals("rabbitTemplateTradeshiftLocalhost5672localhost6672", RabbitBeanNameResolver
				.getRabbitTemplateBeanName(createQueuePropertiesInClusterMode("localhost:5672,localhost:6672", "tradeshift")));
	}

	@Test
	void should_return_the_correct_name_for_default_listener_container_factory_from_properties() {
		assertEquals("containerFactoryDefaultLocalhost5672_1-1-250", RabbitBeanNameResolver
				.getSimpleRabbitListenerContainerFactoryBean(createQueueProperties("localhost", 5672, null)));
	}

	@Test
	void should_return_the_correct_name_for_listener_container_factory_from_properties() {
		TunedRabbitProperties queueProperties = createQueueProperties("localhost", 5672, "tradeshift");
		queueProperties.setConcurrentConsumers(2);
		queueProperties.setMaxConcurrentConsumers(8);
		queueProperties.setPrefetchCount(1);
		assertEquals("containerFactoryTradeshiftLocalhost5672_2-8-1", RabbitBeanNameResolver
				.getSimpleRabbitListenerContainerFactoryBean(queueProperties));
	}
	
	@Test
	void should_return_the_correct_name_for_listener_container_factory_from_properties_with_batch() {
		TunedRabbitProperties queueProperties = createQueueProperties("localhost", 5672, "tradeshift");
		queueProperties.setConcurrentConsumers(2);
		queueProperties.setMaxConcurrentConsumers(8);
		queueProperties.setPrefetchCount(1000);
		queueProperties.setBatchListener(true);
		queueProperties.setConsumerBatchEnabled(true);
		queueProperties.setBatchSize(1000);
		queueProperties.setReceiveTimeout(1000);
		assertEquals("containerFactoryTradeshiftLocalhost5672_2-8-1000-truetrue10001000", RabbitBeanNameResolver
				.getSimpleRabbitListenerContainerFactoryBean(queueProperties));
	}

	@Test
	void should_return_the_correct_name_for_default_container_factory_from_properties_with_cluster_mode() {
		assertEquals("containerFactoryDefaultLocalhost5672localhost6672_1-1-250", RabbitBeanNameResolver
				.getSimpleRabbitListenerContainerFactoryBean(createQueuePropertiesInClusterMode("localhost:5672,localhost:6672", null)));
	}

	@Test
	void should_return_the_correct_name_for_container_factory_from_properties_with_cluster_mode() {
		assertEquals("containerFactoryTradeshiftLocalhost5672localhost6672_1-1-250", RabbitBeanNameResolver
				.getSimpleRabbitListenerContainerFactoryBean(createQueuePropertiesInClusterMode("localhost:5672,localhost:6672", "tradeshift")));
	}


	private TunedRabbitProperties createQueueProperties(String host, int port, String virtualHost) {
		return createQueueProperties(host, port, null, virtualHost);
	}


	private TunedRabbitProperties createQueuePropertiesInClusterMode(String hosts, String virtualHost) {
		return createQueueProperties(null, 0, hosts, virtualHost);
	}

	private TunedRabbitProperties createQueueProperties(String host, int port, String hosts, String virtualHost) {
		TunedRabbitProperties queueProperties = new TunedRabbitProperties();
		if (hosts != null && !hosts.isEmpty()) {
			queueProperties.setClusterMode(true);
			queueProperties.setHosts(hosts);
		}
		queueProperties.setHost(host);
		queueProperties.setPort(port);
		queueProperties.setVirtualHost(virtualHost);
		queueProperties.setQueue("some-queue");
		queueProperties.setExchange("some-exchange");
		queueProperties.setExchangeType("topic");
		queueProperties.setMaxRetriesAttempts(5);
		queueProperties.setQueueRoutingKey("routing.key.test");
		queueProperties.setTtlRetryMessage(3000);
		queueProperties.setPrimary(true);
		queueProperties.setUsername("guest");
		queueProperties.setPassword("guest");
		queueProperties.setDefaultRetryDlq(true);
		queueProperties.setSslConnection(false);
		return queueProperties;
	}
}
