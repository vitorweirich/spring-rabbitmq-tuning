package com.tradeshift.amqp.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Exceptions checking order:
 *
 * <pre>
 * - Should be discarded
 * - Should be sent to retry
 * - Should be sent to DLQ
 * - Otherwise discard
 * </pre>
 *
 * The <code>discardWhen</code> attribute has higher precedence over <code>exceptions</code> attribute.
 */
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface EnableRabbitRetryAfterThrowTunnedRabbitException {

    String event();

}
