use super::Keywords;

pub(super) const MYSQL80_KEYWORDS: MySQL80Keywords = MySQL80Keywords {};

pub(super) struct MySQL80Keywords {}
impl Keywords for MySQL80Keywords {
    fn get_name(&self) -> &'static str {
        "MySQL80"
    }

    fn get_keywords(&self) -> &[&'static str] {
        &[
            "ACCESSIBLE",
            "ADD",
            "ALL",
            "ALTER",
            "ANALYZE",
            "AND",
            "AS",
            "ASC",
            "ASENSITIVE",
            "BEFORE",
            "BETWEEN",
            "BIGINT",
            "BINARY",
            "BLOB",
            "BOTH",
            "BY",
            "CALL",
            "CASCADE",
            "CASE",
            "CHANGE",
            "CHAR",
            "CHARACTER",
            "CHECK",
            "COLLATE",
            "COLUMN",
            "CONDITION",
            "CONNECTION",
            "CONSTRAINT",
            "CONTINUE",
            "CONVERT",
            "CREATE",
            "CROSS",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_USER",
            "CURSOR",
            "DATABASE",
            "DATABASES",
            "DAY_HOUR",
            "DAY_MICROSECOND",
            "DAY_MINUTE",
            "DAY_SECOND",
            "DEC",
            "DECIMAL",
            "DECLARE",
            "DEFAULT",
            "DELAYED",
            "DELETE",
            "DESC",
            "DESCRIBE",
            "DETERMINISTIC",
            "DISTINCT",
            "DISTINCTROW",
            "DIV",
            "DOUBLE",
            "DROP",
            "DUAL",
            "EACH",
            "ELSE",
            "ELSEIF",
            "ENCLOSED",
            "ESCAPED",
            "EXISTS",
            "EXIT",
            "EXPLAIN",
            "FALSE",
            "FETCH",
            "FLOAT",
            "FLOAT4",
            "FLOAT8",
            "FOR",
            "FORCE",
            "FOREIGN",
            "FROM",
            "FULLTEXT",
            "GENERAL",
            "GOTO",
            "GRANT",
            "GROUP",
            "HAVING",
            "HIGH_PRIORITY",
            "HOUR_MICROSECOND",
            "HOUR_MINUTE",
            "HOUR_SECOND",
            "IF",
            "IGNORE",
            "IGNORE_SERVER_IDS",
            "IN",
            "INDEX",
            "INFILE",
            "INNER",
            "INOUT",
            "INSENSITIVE",
            "INSERT",
            "INT",
            "INT1",
            "INT2",
            "INT3",
            "INT4",
            "INT8",
            "INTEGER",
            "INTERVAL",
            "INTO",
            "IS",
            "ITERATE",
            "JOIN",
            "KEY",
            "KEYS",
            "KILL",
            "LABEL",
            "LEADING",
            "LEAVE",
            "LEFT",
            "LIKE",
            "LIMIT",
            "LINEAR",
            "LINES",
            "LOAD",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "LOCK",
            "LONG",
            "LONGBLOB",
            "LONGTEXT",
            "LOOP",
            "LOW_PRIORITY",
            "MASTER_HEARTBEAT_PERIOD",
            "MASTER_SSL_VERIFY_SERVER_CERT",
            "MATCH",
            "MAXVALUE",
            "MEDIUMBLOB",
            "MEDIUMINT",
            "MEDIUMTEXT",
            "MIDDLEINT",
            "MINUTE_MICROSECOND",
            "MINUTE_SECOND",
            "MOD",
            "MODIFIES",
            "NATURAL",
            "NO_WRITE_TO_BINLOG",
            "NOT",
            "NULL",
            "NUMERIC",
            "ON",
            "OPTIMIZE",
            "OPTION",
            "OPTIONALLY",
            "OR",
            "ORDER",
            "OUT",
            "OUTER",
            "OUTFILE",
            "PARTITION",
            "PRECISION",
            "PRIMARY",
            "PROCEDURE",
            "PURGE",
            "RAID0",
            "RANGE",
            "READ",
            "READ_WRITE",
            "READS",
            "REAL",
            "RECURSIVE",
            "REFERENCES",
            "REGEXP",
            "RELEASE",
            "RENAME",
            "REPEAT",
            "REPLACE",
            "REQUIRE",
            "RESIGNAL",
            "RESTRICT",
            "RETURN",
            "REVOKE",
            "RIGHT",
            "RLIKE",
            "ROWS",
            "SCHEMA",
            "SCHEMAS",
            "SECOND_MICROSECOND",
            "SELECT",
            "SENSITIVE",
            "SEPARATOR",
            "SET",
            "SHOW",
            "SIGNAL",
            "SLOW",
            "SMALLINT",
            "SONAME",
            "SPATIAL",
            "SPECIFIC",
            "SQL",
            "SQL_BIG_RESULT",
            "SQL_CALC_FOUND_ROWS",
            "SQL_SMALL_RESULT",
            "SQLEXCEPTION",
            "SQLSTATE",
            "SQLWARNING",
            "SSL",
            "STARTING",
            "STRAIGHT_JOIN",
            "TABLE",
            "TERMINATED",
            "THEN",
            "TINYBLOB",
            "TINYINT",
            "TINYTEXT",
            "TO",
            "TRAILING",
            "TRIGGER",
            "TRUE",
            "UNDO",
            "UNION",
            "UNIQUE",
            "UNLOCK",
            "UNSIGNED",
            "UPDATE",
            "USAGE",
            "USE",
            "USING",
            "UTC_DATE",
            "UTC_TIME",
            "UTC_TIMESTAMP",
            "VALUES",
            "VARBINARY",
            "VARCHAR",
            "VARCHARACTER",
            "VARYING",
            "WHEN",
            "WHERE",
            "WHILE",
            "WITH",
            "WRITE",
            "X509",
            "XOR",
            "YEAR_MONTH",
            "ZEROFILL",
            "ADMIN",
            "ARRAY",
            "CUBE",
            "CUME_DIST",
            "DENSE_RANK",
            "EMPTY",
            "EXCEPT",
            "FIRST_VALUE",
            "FUNCTION",
            "GROUPING",
            "GROUPS",
            "JSON_TABLE",
            "LAG",
            "LAST_VALUE",
            "LATERAL",
            "LEAD",
            "MEMBER",
            "NTH_VALUE",
            "NTILE",
            "OF",
            "OVER",
            "PERCENT_RANK",
            "PERSIST",
            "PERSIST_ONLY",
            "RANK",
            "RECURSIVE",
            "ROW",
            "ROWS",
            "ROW_NUMBER",
            "SYSTEM",
            "WINDOW",
        ]
    }
}
