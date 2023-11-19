package com.spaulding.tools.Archive.services;

import com.spaulding.tools.Archive.Archive;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;

public class DBEncryptionService extends Archive {
    private String STANDARD_ENCRYPTION_KEY;

    public DBEncryptionService(String className, String url, String userName, String credentials) throws SQLException {
        super("DB-Encryption-Service", className, url, userName, credentials);
    }

    public DBEncryptionService(JdbcTemplate jdbc) throws SQLException {
        super("DB-Encryption-Service", jdbc);
    }

    @Override
    protected void setup() throws SQLException {
        registerNewKey("general-key");
        STANDARD_ENCRYPTION_KEY = getKey("general-key");
    }

    public void registerNewKey(String keyName) throws SQLException {
        if (getKey(keyName) == null) {
            execute(Archive.SYSADMIN, Archive.SYSTEM_PROPERTIES_TABLE_INSERT, new Object[]{ name, keyName, cypher.createKey() }, STANDARD_ENCRYPTION_KEY);
        }
    }

    public String getKey(String keyName) throws SQLException {
        List<Row> rows = execute(Archive.SYSADMIN, Archive.SYSTEM_PROPERTIES_TABLE_SELECT, new Object[]{ name, keyName }, STANDARD_ENCRYPTION_KEY);
        if (rows.isEmpty()) {
            return null;
        }

        return (String) rows.get(0).getResult(2);
    }

    public List<Row> getAllKeys() throws SQLException {
        return execute(Archive.SYSADMIN, Archive.SYSTEM_PROPERTIES_TABLE_SELECT_ALL, new Object[]{ name }, STANDARD_ENCRYPTION_KEY);
    }
}
