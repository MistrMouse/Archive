package com.spaulding.tools.Archive;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestArchive {
    @Test
    public void rawSqlTest() throws SQLException, IOException {
        TestArchiveService testArchiveService = new TestArchiveService();
        assertEquals("[auth, Test, Y]", testArchiveService.toStringTest());
        Files.delete(Paths.get("Test.db"));
    }
}
