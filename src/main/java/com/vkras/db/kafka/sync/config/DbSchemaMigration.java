package com.vkras.db.kafka.sync.config;

import com.vkras.db.kafka.sync.entity.KafkaDBOffsetEntity;
import jakarta.persistence.EntityManager;
import lombok.AllArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.Resource;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * init Db with kafka offset messages {@link KafkaDBOffsetEntity}
 */
@AllArgsConstructor
public class DbSchemaMigration implements ApplicationListener<ApplicationReadyEvent> {

    private Resource initScript;
    private final EntityManager entityManager;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        if (!entityManager.getMetamodel().getEntities().contains(KafkaDBOffsetEntity.class)) {
            entityManager.createNativeQuery(asString(initScript)).executeUpdate();
        }
    }

    private String asString(Resource resource) {
        try (Reader reader = new InputStreamReader(resource.getInputStream(), UTF_8)) {
            return FileCopyUtils.copyToString(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
