package org.apache.flink.connector.phoenix.source;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.phoenix.DataSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 */
public class PhoenixRowDataLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixRowDataLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final String dbURL;

    private final String query;
    private final DataType[] keyTypes;
    private final String[] keyNames;

    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final JdbcRowConverter lookupKeyRowConverter;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    private transient HikariDataSource hikariDataSource;


    public PhoenixRowDataLookupFunction(JdbcOptions options,
                                        JdbcLookupOptions lookupOptions,
                                        String[] fieldNames,
                                        DataType[] fieldTypes,
                                        String[] keyNames,
                                        RowType rowType) {
        checkNotNull(options, "No JdbcOptions supplied.");
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        this.dbURL = options.getDbURL();
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyNames = keyNames;
        this.keyTypes = Arrays.stream(keyNames)
                .map(s -> {
                    checkArgument(nameList.contains(s),
                            "keyName %s can't find in fieldNames %s.", s, nameList);
                    return fieldTypes[nameList.indexOf(s)];
                }).toArray(DataType[]::new);

        this.jdbcDialect = new PhoenixDialect();
        this.query = jdbcDialect.getSelectFromStatement(options.getTableName(), fieldNames, keyNames);
        this.jdbcRowConverter = jdbcDialect.getRowConverter(rowType);
        this.lookupKeyRowConverter = jdbcDialect.getRowConverter(RowType.of(Arrays.stream(keyTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new)));

        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        LOG.info("start open ...");
        super.open(context);
        LOG.info("Phoenix JDBC URL: {}", dbURL);
        LOG.info("Query: {}", query);
        hikariDataSource = DataSource.newHikariDataSource(dbURL, null, null);
        LOG.info("end open ...");
    }

    public void eval(Object... keys) {
        RowData keyRow = GenericRowData.of(keys);

        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            Connection connection = null;
            FieldNamedPreparedStatement statement = null;
            try {
                connection = hikariDataSource.getConnection();
                statement = FieldNamedPreparedStatement.prepareStatement(connection, query, keyNames);
                statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        collect(jdbcRowConverter.toInternal(resultSet));
                    }
                }
                break;
            } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (Exception e) {
                        LOG.info("JDBC statement could not be closed: " + e.getMessage());
                    } finally {
                        statement = null;
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {
                        LOG.info("JDBC connection could not be closed: " + e.getMessage());
                    } finally {
                        connection = null;
                    }
                }
            }
        }

    }


    @Override
    public void close() {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }
    }
}
