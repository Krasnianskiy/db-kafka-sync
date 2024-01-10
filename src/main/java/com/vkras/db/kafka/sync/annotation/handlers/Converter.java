package com.vkras.db.kafka.sync.annotation.handlers;

@FunctionalInterface
public interface Converter<S, T> {
    T convert(S source);
}
