package com.vkras.db.kafka.sync.annotation;

import com.vkras.db.kafka.sync.annotation.handlers.Converter;
import com.vkras.db.kafka.sync.annotation.handlers.DefaultConverter;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ExternalTable {
    Class<? extends Converter> converter() default DefaultConverter.class;
}
