package com.spaulding.tools.Archive;

import java.sql.SQLException;
import java.sql.Types;

public class TestArchiveService extends Archive {
    public TestArchiveService() throws SQLException {
        super("Test", "org.sqlite.JDBC", "jdbc:sqlite:Test.db", "admin", "p455w0rd");
    }

    @Override
    protected void setup() throws SQLException {
        execute(SYSADMIN, AUTH_TABLE_INSERT, new Object[]{ AUTH_TABLE, "Test", 'Y' });
    }

    public String toStringTest() throws SQLException {
        return execute("Test", "SELECT * FROM " + AUTH_TABLE, null, (Integer[]) null).get(0).toString();
    }
}
