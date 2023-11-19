package com.spaulding.tools.Archive.services;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

public class TestDBEncryptionService {
    private static DBEncryptionKeyService dbEncryptionService;

    @BeforeAll
    public static void init() throws SQLException {
        dbEncryptionService = new DBEncryptionKeyService("org.sqlite.JDBC", "jdbc:sqlite:Test.db", "admin", "p455w0rd");
    }

    @Test
    public void registerNewKeyTest() throws SQLException, IOException {
        dbEncryptionService.registerNewKey("Test");
        String key = dbEncryptionService.getKey("Test");
        assertNotNull(key);
        assertEquals(key, dbEncryptionService.getKey("Test"));
        Files.delete(Paths.get("Test.db"));
    }

}
