package com.vkras.db.kafka.sync.config;

import com.vkras.db.kafka.sync.service.DbKafkaProducer;
import com.vkras.db.kafka.sync.service.KafkaSyncEntityListener;
import jakarta.persistence.EntityManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

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

    /*
    @Bean
    public KafkaProducer<String, Object> producer(){
        Properties properties = new Properties();
        properties.putAll(kafkaProperties.getProperties());
        return new KafkaProducer<>(properties);
    }
     */

    @Bean
    @ConditionalOnProperty(name = "com.spring.kafka.db.synchronize.initDb")
    @ConditionalOnBean(EntityManager.class)
    public DbSchemaMigration dbSchemaMigration(@Value("classpath:db/init_offset_table.sql") Resource initScript,
                                               EntityManager entityManager){
        return new DbSchemaMigration(initScript, entityManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public DbKafkaProducer dbKafkaProducer(){
        Properties properties = new Properties();
        properties.putAll(kafkaProperties.getProperties());
        return new DbKafkaProducer(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaSyncEntityListener entityListener(){
        return new KafkaSyncEntityListener(synchronizedProperties, dbKafkaProducer());
    }
}
