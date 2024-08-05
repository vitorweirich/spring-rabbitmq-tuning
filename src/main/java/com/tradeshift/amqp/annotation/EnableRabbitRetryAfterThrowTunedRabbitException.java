package com.tradeshift.amqp.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * Handle the Specific <code>TunedRabbitException</code>, to let you decida when to send to DLQ or to RETRY.
 * Designed specifically to work with <code>BatchListeners</code>
 * 
 */
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface EnableRabbitRetryAfterThrowTunedRabbitException {

    String event();

}
