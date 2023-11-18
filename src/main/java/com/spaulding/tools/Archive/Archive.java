package com.spaulding.tools.Archive;

import com.spaulding.tools.Cypher.Cypher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.lang.NonNull;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Archive {
    protected static final String AUTH_TABLE = "auth";
    protected static final String AUTH_REF_TABLE = "auth_ref";
    protected static final String SYSTEM_PROPERTIES_TABLE = "system_properties";
    protected static final String SELECT_FOLLOWER = "-Select";
    protected static final String INSERT_FOLLOWER = "-Insert";
    protected static final String UPDATE_FOLLOWER = "-Update";
    protected static final String AUTH_REF_TABLE_SELECT = AUTH_REF_TABLE + SELECT_FOLLOWER;
    protected static final String AUTH_REF_TABLE_INSERT = AUTH_REF_TABLE + INSERT_FOLLOWER;
    protected static final String AUTH_REF_TABLE_UPDATE = AUTH_REF_TABLE + UPDATE_FOLLOWER;
    protected static final String AUTH_TABLE_SELECT = AUTH_TABLE + SELECT_FOLLOWER;
    protected static final String AUTH_TABLE_INSERT = AUTH_TABLE + INSERT_FOLLOWER;
    protected static final String AUTH_TABLE_UPDATE = AUTH_TABLE + UPDATE_FOLLOWER;
    protected static final String SYSTEM_PROPERTIES_TABLE_SELECT = SYSTEM_PROPERTIES_TABLE + SELECT_FOLLOWER;
    protected static final String SYSTEM_PROPERTIES_TABLE_INSERT = SYSTEM_PROPERTIES_TABLE + INSERT_FOLLOWER;
    protected static final String SYSTEM_PROPERTIES_TABLE_UPDATE = SYSTEM_PROPERTIES_TABLE + UPDATE_FOLLOWER;
    protected static final String SYSADMIN = "SYSADMIN";
    protected static final String TYPE_OPERATION = "OPERATION";
    protected static final String TYPE_TABLE = "TABLE";
    protected static final String tableRegex = "(?<" + TYPE_OPERATION + ">(CREATE|DROP|UPDATE|INSERT|FROM|JOIN))( TABLE IF NOT EXISTS | TABLE | OR IGNORE INTO | INTO | )(?<" + TYPE_TABLE + ">(.*?))( ORDER| WHERE| JOIN| ON| VALUES| \\(| SET|;)";
    protected static final Pattern tablePattern = Pattern.compile(tableRegex, Pattern.CASE_INSENSITIVE);

    public static JdbcTemplate initJdbcTemplate(String className, String url, String userName, String credentials) {
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
        driverManagerDataSource.setDriverClassName(className);
        driverManagerDataSource.setUrl(url);
        driverManagerDataSource.setUsername(userName);
        driverManagerDataSource.setPassword(credentials);
        return new JdbcTemplate(driverManagerDataSource);
    }

    public final String name;
    protected final JdbcTemplate jdbc;
    protected final Cypher cypher;

    public Archive(@NonNull String name, @NonNull String className, @NonNull String url, @NonNull String userName, @NonNull String credentials) throws SQLException {
        this(name, className, url, userName, credentials, null);
    }

    public Archive(@NonNull String name, @NonNull String className, @NonNull String url, @NonNull String userName, @NonNull String credentials, Cypher cypher) throws SQLException {
        this(
                name,
                initJdbcTemplate(className, url, userName, credentials),
                cypher
        );
    }

    public Archive(@NonNull String name, @NonNull JdbcTemplate jdbc) throws SQLException {
        this(name, jdbc, null);
    }

    public Archive(@NonNull String name, @NonNull JdbcTemplate jdbc, Cypher cypher) throws SQLException {
        this.jdbc = jdbc;
        this.name = name;
        this.cypher = cypher == null ? new Cypher() : cypher;
        init();
        setup();
    }

    protected abstract void setup() throws SQLException;

    private void init() throws SQLException {
        initAuthRef();
        initAuth();
        initSystemProperties();
    }

    private void initAuthRef() throws SQLException {
        jdbc.execute("CREATE TABLE IF NOT EXISTS " + AUTH_REF_TABLE + " (name VARCHAR PRIMARY KEY, ref VARCHAR, numOfArgs INTEGER, argTypes VARCHAR, UNIQUE(ref))");

        String AUTH_REF_INSERTION_SQL = "INSERT OR IGNORE INTO " + Archive.AUTH_REF_TABLE + " VALUES (?, ?, ?, ?)";
        Object[] args = new Object[] {
                AUTH_REF_TABLE_INSERT,
                AUTH_REF_INSERTION_SQL,
                4,
                "String,String,Integer,String"
        };
        Integer[] argTypes = new Integer[] {
                Types.VARCHAR,
                Types.VARCHAR,
                Types.INTEGER,
                Types.VARCHAR
        };
        execute(SYSADMIN, AUTH_REF_INSERTION_SQL, args, argTypes);

        args = new Object[] {
                AUTH_REF_TABLE_SELECT,
                "SELECT * FROM " + AUTH_REF_TABLE + " WHERE name = ?",
                1,
                "String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                AUTH_REF_TABLE_UPDATE,
                "UPDATE " + AUTH_REF_TABLE + " SET type = ?, ref = ?, numOfArgs = ?, argTypes = ? WHERE name = ?",
                5,
                "String,String,Integer,String,String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);
    }

    private void initAuth() throws SQLException {
        jdbc.execute("CREATE TABLE IF NOT EXISTS " + AUTH_TABLE  + " (name VARCHAR, auth VARCHAR, active CHAR, UNIQUE(name, auth), FOREIGN KEY(name) REFERENCES " + AUTH_REF_TABLE + "(name))");

        Object[] args = new Object[] {
                AUTH_TABLE_SELECT,
                "SELECT * FROM " + AUTH_TABLE + " WHERE name = ? AND auth = ? AND active = 'Y'",
                2,
                "String,String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                AUTH_TABLE_INSERT,
                "INSERT OR IGNORE INTO " + AUTH_TABLE + " VALUES (?, ?, ?)",
                3,
                "String,String,Character"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                AUTH_TABLE_UPDATE,
                "UPDATE " + AUTH_TABLE + " SET value = ? WHERE name = ? AND key = ?",
                3,
                "String,String,String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);
    }

    private void initSystemProperties() throws SQLException {
        jdbc.execute("CREATE TABLE IF NOT EXISTS " + SYSTEM_PROPERTIES_TABLE + " (name VARCHAR, key VARCHAR, value VARCHAR, UNIQUE(name, key))");

        Object[] args = new Object[] {
                SYSTEM_PROPERTIES_TABLE_SELECT,
                "SELECT * FROM " + SYSTEM_PROPERTIES_TABLE + " WHERE name = ? AND key = ?",
                2,
                "String,String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                SYSTEM_PROPERTIES_TABLE_INSERT,
                "INSERT OR IGNORE INTO " + SYSTEM_PROPERTIES_TABLE + " VALUES (?, ?, ?)",
                3,
                "String,String,String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                SYSTEM_PROPERTIES_TABLE_UPDATE,
                "UPDATE " + SYSTEM_PROPERTIES_TABLE + " SET value = ? WHERE name = ? AND key = ?",
                3,
                "String,String,String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);
    }

    private PreparedStatementCreator getPreparedStatementCreator(@NonNull String sql, Object[] args, Integer[] argTypes) {
        PreparedStatementCreatorFactory pscf = new PreparedStatementCreatorFactory(sql);
        for (Integer argType : argTypes) {
            pscf.addParameter(new SqlParameter(argType));
        }
        return pscf.newPreparedStatementCreator(args);
    }

    private boolean isNotAuthorized(@NonNull String auth, @NonNull String ref) {
        try {
            return !auth.equals(SYSADMIN) && execute(SYSADMIN, AUTH_TABLE_SELECT, new Object[]{ref, auth}).isEmpty();
        }
        catch (SQLException e) {
            return true;
        }
    }

    private String rawSQLCheck(@NonNull String auth, @NonNull String sql) {
        Matcher matcher = tablePattern.matcher(sql);
        if (!auth.equals(SYSADMIN)) {
            while (matcher.find()) {
                String operation = matcher.group(TYPE_OPERATION).toUpperCase();
                String table = matcher.group(TYPE_TABLE);
                if (operation.equals("CREATE") || operation.equals("DROP")) {
                    return "DB: " + name + ", AUTH: " + auth + ", SQL: { " + sql + " } - [Invalid] Not authorized to perform a CREATE or DROP.";
                } else if (!table.equalsIgnoreCase("SELECT")) {
                    if (isNotAuthorized(auth, table)) {
                        return "DB: " + name + ", AUTH: " + auth + ", TABLE: " + table + ", SQL: { " + sql + " } - [Invalid] Not authorized to access the given table.";
                    }
                }
            }
        }

        return "DB: " + name + ", AUTH: " + auth + ", SQL: { " + sql + " } - [Valid] Allowed to be executed.";
    }

    private List<Row> decodeRows(List<Row> rows, String key) {
        List<Row> results = new ArrayList<>();

        for (Row row : rows) {
            Row result = new Row(row.getNumberOfColumns());
            for (int i = 0; i < row.getNumberOfColumns(); i++) {
                Object obj = row.getResult(i);
                if (obj instanceof String) {
                    if (key != null) {
                        obj = cypher.decode((String) obj, key);
                    }
                }
                result.setColumnName(i, row.getColumnName(i));
                result.setColumnType(i, row.getColumnType(i));
                result.setResult(i, obj);
            }
            results.add(result);
        }

        return results;
    }

    protected List<Row> execute(@NonNull String auth, @NonNull String sql, Object[] args, Integer[] argTypes) throws SQLException {
        return execute(auth, sql, args, argTypes, null);
    }

    protected List<Row> execute(@NonNull String auth, @NonNull String sql, Object[] args, Integer[] argTypes, String key) throws SQLException {
        if (argTypes == null) {
            argTypes = new Integer[] {};
        }

        if (sql.charAt(sql.length() - 1) != ';') {
            sql += ';';
        }

        String rawValidationResult = rawSQLCheck(auth, sql);
        if (rawValidationResult.contains("[Invalid]")) {
            throw new SQLException(rawValidationResult);
        }

        if (sql.toUpperCase().startsWith("SELECT")) {
            return decodeRows(jdbc.query(
                    getPreparedStatementCreator(sql, args, argTypes),
                    (rs, rowNum) -> new Row(rs)
            ), key);
        }
        else {
            Object[] arguments = new Object[args == null ? 0 : args.length];
            for (int i = 0; i < arguments.length; i++) {
                Object arg = args[i];
                if (arg instanceof String) {
                    if (key != null) {
                        arg = cypher.encode((String) arg, key);
                    }
                }
                arguments[i] = arg;
            }

            jdbc.update(getPreparedStatementCreator(sql, arguments, argTypes));
        }

        return null;
    }

    protected List<Row> execute(@NonNull String auth, @NonNull String ref) throws SQLException {
        return execute(auth, ref, null, (String) null);
    }

    protected List<Row> execute(@NonNull String auth, @NonNull String ref, String key) throws SQLException {
        return execute(auth, ref, null, key);
    }

    protected List<Row> execute(@NonNull String auth, @NonNull String ref, Object[] args) throws SQLException {
        return execute(auth, ref, args, (String) null);
    }

    protected List<Row> execute(@NonNull String auth, @NonNull String ref, Object[] args, String key) throws SQLException {
        if (args == null) {
            args = new Object[] {};
        }

        if (isNotAuthorized(auth, ref)) {
            throw new SQLException("DB: " + ref + ", AUTH: " + auth + ", REF: " + ref + ", ARGS: { " + Arrays.toString(args) + " } - [Invalid] Not authorized to access the given ref or it does not exist.");
        }

        List<Row> rows = jdbc.query(
                getPreparedStatementCreator("SELECT * FROM " + AUTH_REF_TABLE + " WHERE name = ?", new Object[]{ ref }, new Integer[]{ Types.VARCHAR }),
                (rs, rowNum) -> new Row(rs)
        );

        Row refInfo = rows.get(0);
        String sql = (String) refInfo.getResult(1);
        int numOfArgs = (Integer) refInfo.getResult(2);
        if (numOfArgs != args.length) {
            throw new SQLException("DB: " + ref + ", AUTH: " + auth + ", REF: " + ref + ", ARGS: { " + Arrays.toString(args) + " } - [Invalid] Argument Amount - " + numOfArgs + " was expected, but received " + args.length + ".");
        }

        Integer[] argTypes = new Integer[numOfArgs];
        String typeDelim = (String) refInfo.getResult(3);
        if (typeDelim != null && !typeDelim.isEmpty()) {
            String[] typeNames = typeDelim.split(",");
            for (int i = 0; i < args.length; i++) {
                String argTypeName = (args[i] == null) ? null : args[i].getClass().getSimpleName();
                String typeName = typeNames[i];
                if (argTypeName != null && !argTypeName.equals(typeName)) {
                    throw new SQLException("DB: " + ref + ", AUTH: " + auth + ", REF: " + ref + ", ARGS: { " + Arrays.toString(args) + " } - [Invalid] Argument Type - " + typeName + " was expected, but received " + argTypeName + ".");
                }

                argTypes[i] = convertToSQLType(typeName);
            }
        }

        Object[] arguments = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg instanceof String) {
                if (key != null) {
                    arg = cypher.encode((String) arg, key);
                }
            }
            arguments[i] = arg;
        }

        if (sql.toUpperCase().startsWith("SELECT")) {
            return decodeRows(jdbc.query(
                    getPreparedStatementCreator(sql, arguments, argTypes),
                    (rs, rowNum) -> new Row(rs)
            ), key);
        }
        else {
            jdbc.update(getPreparedStatementCreator(sql, arguments, argTypes));
        }

        return null;
    }

    private Integer convertToSQLType(String type) {
        return switch (type) {
            case "Integer" -> Types.INTEGER;
            case "Character" -> Types.CHAR;
            case "Double" -> Types.DOUBLE;
            case "Float" -> Types.FLOAT;
            default -> Types.VARCHAR;
        };
    }

    public static class Row {
        private final String[] cNames;
        private final Integer[] cTypes;
        private final Object[] results;

        public Row(int numOfColumns) {
            cNames = new String[numOfColumns];
            cTypes = new Integer[numOfColumns];
            results = new Object[numOfColumns];
        }

        public Row(ResultSet rs) throws SQLException {
            ResultSetMetaData rsmd = rs.getMetaData();
            int numOfColumns = rsmd.getColumnCount();
            cNames = new String[numOfColumns];
            cTypes = new Integer[numOfColumns];
            for (int i = 0; i < numOfColumns; i++) {
                cNames[i] = rsmd.getColumnName(i + 1);
                cTypes[i] = rsmd.getColumnType(i + 1);
            }

            results = new Object[cNames.length];
            for (int i = 0; i < results.length; i++) {
                results[i] = rs.getObject(i + 1);
            }
        }

        public void setColumnName(int column, String value) {
            cNames[column] = value;
        }

        public void setColumnType(int column, int value) {
            cTypes[column] = value;
        }

        public void setResult(int column, Object value) {
            results[column] = value;
        }

        public int getNumberOfColumns() {
            return cNames.length;
        }

        public String getColumnName(int colNum) {
            return cNames[colNum];
        }

        public int getColumnType(int colNum) {
            return cTypes[colNum];
        }

        public Object getResult(int colNum) {
            return results[colNum];
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (int i = 0; i < cNames.length; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(results[i]);
            }
            sb.append("]");
            return sb.toString();
        }
    }
}