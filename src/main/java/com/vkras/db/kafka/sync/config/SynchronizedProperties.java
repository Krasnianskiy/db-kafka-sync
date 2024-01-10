package com.vkras.db.kafka.sync.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "com.spring.kafka.db.synchronize")
@Getter
@Setter
public class SynchronizedProperties {
    private String scanPackage;
    private Boolean initDb = true;
}
