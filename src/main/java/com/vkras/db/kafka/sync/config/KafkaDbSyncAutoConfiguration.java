package com.vkras.db.kafka.sync.config;

import com.vkras.db.kafka.sync.service.KafkaSyncEntityListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

@AutoConfiguration
@EnableConfigurationProperties({SynchronizedProperties.class, KafkaProperties.class})
public class KafkaDbSyncAutoConfiguration {

    private SynchronizedProperties synchronizedProperties;

    private KafkaProperties kafkaProperties;

    public KafkaDbSyncAutoConfiguration(SynchronizedProperties synchronizedProperties,
                                        KafkaProperties kafkaProperties) {
        this.synchronizedProperties = synchronizedProperties;
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public KafkaProducer<String, Object> producer(){
        Properties properties = new Properties();
        properties.putAll(kafkaProperties.getProperties());
        return new KafkaProducer<>(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaSyncEntityListener entityListener(){
        return new KafkaSyncEntityListener(synchronizedProperties, producer());
    }
}
