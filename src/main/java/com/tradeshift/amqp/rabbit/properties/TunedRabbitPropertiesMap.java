package com.tradeshift.amqp.rabbit.properties;

import java.util.HashMap;
import java.util.stream.Stream;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("spring.rabbitmq.custom")
public class TunedRabbitPropertiesMap extends HashMap<String, TunedRabbitProperties> {

    public Stream<Entry<String, TunedRabbitProperties>> stream() {
    	this.remove("shared");
        return entrySet().stream();
    }

}
