package com.vkras.db.kafka.sync.service;

public interface SyncTables {
    /**
     * All empty tables Sync
     */
    void syncAllTables();

    /**
     * single table sync
     * @param tableName - table name
     */
    void syncTable(String tableName);
}
