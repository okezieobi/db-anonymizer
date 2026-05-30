package com.anonymizer.config;

import java.util.Map;

public record MaskingConfig(
        String dbUrl,
        String dbUser,
        String dbPassword,
        String targetTable,
        Map<String, MaskingStrategy> columnsToMask) {
}