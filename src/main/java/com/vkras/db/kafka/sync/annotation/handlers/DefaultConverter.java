package com.vkras.db.kafka.sync.annotation.handlers;

public class DefaultConverter implements Converter<Object, Object>{
    @Override
    public Object convert(Object source) {
        return source;
    }
}
