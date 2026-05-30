package com.anonymizer.engine;

import com.anonymizer.config.MaskingConfig;
import com.anonymizer.config.MaskingStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class AnonymizerEngine implements Runnable {
    private final MaskingConfig config;
    private final HikariDataSource dataSource;

    public AnonymizerEngine(MaskingConfig config) {
        this.config = config;

        // Explicit connection pool initialization
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.dbUrl());
        hikariConfig.setUsername(config.dbUser());
        hikariConfig.setPassword(config.dbPassword());
        hikariConfig.setMaximumPoolSize(5); // Conservative pool for single-worker migration

        this.dataSource = new HikariDataSource(hikariConfig);
    }

    @Override
    public void run() {
        // Enforce cursor-based streaming by disabling auto-commit on the connection
        String selectSql = "SELECT * FROM " + config.targetTable();

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false); // CRUCIAL for cursor streaming in PostgreSQL

            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                // Hint to the driver to stream 50 rows at a time over the network instead of
                // pulling all
                stmt.setFetchSize(50);

                try (ResultSet rs = stmt.executeQuery()) {
                    System.out.println("Beginning streaming extraction pipeline...");
                    long processedCount = 0;

                    while (rs.next()) {
                        processRow(rs);
                        processedCount++;
                        if (processedCount % 100 == 0) {
                            System.out.printf("Processed %d rows...%n", processedCount);
                        }
                    }
                    System.out.printf("Pipeline complete. Total rows processed smoothly: %d%n", processedCount);
                }
            }
        } catch (SQLException e) {
            System.err.println("Database stream execution failed!");
            e.printStackTrace();
        } finally {
            dataSource.close();
        }
    }

    private void processRow(ResultSet rs) throws SQLException {
        // Dynamic evaluation of row data matching your map settings
        for (Map.Entry<String, MaskingStrategy> entry : config.columnsToMask().entrySet()) {
            String columnName = entry.getKey();
            MaskingStrategy strategy = entry.getValue();

            String originalValue = rs.getString(columnName);
            if (originalValue == null)
                continue;

            // Pattern matching to execute our transformation algorithms
            String maskedValue = switch (strategy) {
                case EMAIL -> maskEmail(originalValue);
                case PHONE_NUMBER -> "080" + UUID.randomUUID().toString().substring(0, 7).replaceAll("[^0-9]", "3");
                case RANDOM_STRING -> UUID.randomUUID().toString().substring(0, 12);
            };

            // For this phase, we print the mapping transformation explicitly to verify
            // logic
            System.out.printf("[Column: %s] Mapped Data: %s -> %s%n", columnName, originalValue, maskedValue);
        }
    }

    private String maskEmail(String email) {
        if (!email.contains("@")) {
            return "masked_" + UUID.randomUUID().toString().substring(0, 5) + "@example.com";
        }
        String[] parts = email.split("@");
        String localPart = parts[0];
        String domain = parts[1];

        String maskedLocal = localPart.charAt(0) + "xxxx" + localPart.charAt(localPart.length() - 1);
        return maskedLocal + "@" + domain;
    }
}