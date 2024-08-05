package com.tradeshift.amqp.annotation;

import java.lang.reflect.Method;
import java.util.Objects;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;
import com.tradeshift.amqp.rabbit.retry.QueueRetryComponent;

@Aspect
@Configuration
public class EnableRabbitRetryAfterThrowTunedRabbitExceptionAspect {

    private final QueueRetryComponent queueRetryComponent;
    private final TunedRabbitPropertiesMap rabbitCustomPropertiesMap;

    @Autowired
    public EnableRabbitRetryAfterThrowTunedRabbitExceptionAspect(QueueRetryComponent queueRetryComponent, TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        this.queueRetryComponent = queueRetryComponent;
        this.rabbitCustomPropertiesMap = rabbitCustomPropertiesMap;
    }

    @Around("com.tradeshift.amqp.annotation.CommonJoinPointConfig.enableRabbitRetryAfterThrowTunnedRabbitException()")
    public void retryOrDlqAfterTwrow(ProceedingJoinPoint joinPoint) throws Throwable {
    	try {
    		joinPoint.proceed();
    	} catch (TunedRabbitException tunnedEx) {
    		Method method = getMethod(joinPoint);
    		EnableRabbitRetryAfterThrowTunedRabbitException annotation = method.getAnnotation(EnableRabbitRetryAfterThrowTunedRabbitException.class);
    		TunedRabbitProperties properties = getPropertiesByAnnotationEvent(annotation);
			tunnedEx.getToDlq().forEach(message -> queueRetryComponent.sendToDlq(message, properties));
			tunnedEx.getToRetry().forEach(message -> queueRetryComponent.sendToRetryOrDlq(message, properties));
    	}
    }

    private Method getMethod(JoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        return signature.getMethod();
    }

    private TunedRabbitProperties getPropertiesByAnnotationEvent(EnableRabbitRetryAfterThrowTunedRabbitException annotation) {
        String queueProperty = annotation.event();
        TunedRabbitProperties properties = rabbitCustomPropertiesMap.get(queueProperty);
        if (Objects.isNull(properties)) {
            throw new NoSuchBeanDefinitionException(String.format("Any bean with name %s was found", queueProperty));
        }
        return properties;
    }

}
