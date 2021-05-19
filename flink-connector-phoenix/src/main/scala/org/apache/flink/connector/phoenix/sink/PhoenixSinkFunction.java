package org.apache.flink.connector.phoenix.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.phoenix.Constans;
import org.apache.flink.connector.phoenix.PhoenixOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
public class PhoenixSinkFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixSinkFunction.class);

    public static final int CONNECTION_CHECK_TIMEOUT_SECONDS = 60;

    private PhoenixOptions phoenixOptions;

    private String[] fieldNames;

    private DataType[] fieldDataTypes;

    private int[] keyIndices;


    private int maxRetryTimes = 3;

    private String upsertSQL = null;
    private String delSQL = null;
    private Connection dbConn;
    private PreparedStatement upsertPstmt;
    private PreparedStatement delPstmt;


    public PhoenixSinkFunction(PhoenixOptions phoenixOptions, String[] fieldNames, DataType[] fieldDataTypes, int[] keyIndices) {
        this.phoenixOptions = phoenixOptions;
        this.fieldNames = fieldNames;
        this.fieldDataTypes = fieldDataTypes;
        this.keyIndices = keyIndices;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("start open ...");
        super.open(parameters);
        upsertSQL = buildUpsetSQL(phoenixOptions, fieldNames);
        delSQL = buildDelSQL(phoenixOptions, fieldNames, keyIndices);

        try {
            establishConnectionAndStatement();
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }

        LOG.info("end open.");
    }


    private String buildUpsetSQL(PhoenixOptions options, String[] fieldNames) {
        StringBuilder filedBuffer = new StringBuilder();
        StringBuilder valueBuffer = new StringBuilder();

        for (String field : fieldNames) {
            if (filedBuffer.toString().length() > 0) {
                filedBuffer.append(",");
            }
            filedBuffer.append(field);

            if (valueBuffer.toString().length() > 0) {
                valueBuffer.append(",");
            }
            valueBuffer.append("?");
        }

        StringBuilder builder = new StringBuilder();
        builder.append("UPSERT INTO ")
                .append(options.getTableName())
                .append(" (")
                .append(filedBuffer.toString())
                .append(") values (")
                .append(valueBuffer.toString())
                .append(")");

        return builder.toString();
    }

    private String buildDelSQL(PhoenixOptions options, String[] fieldNames, int[] keyIndices) {
        StringBuilder builder = new StringBuilder();

        builder.append("DELETE FROM ")
                .append(options.getTableName())
                .append(" WHERE ");
        for (int i = 0; i < keyIndices.length; i++) {
            if (i > 0) builder.append(" AND ");
            builder.append(fieldNames[i] + "=? ");
        }

        return builder.toString();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        RowKind rowKind = value.getRowKind();
        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            PreparedStatement pstmt = null;
            try {
                if (RowKind.INSERT == rowKind || RowKind.UPDATE_BEFORE == rowKind || RowKind.UPDATE_AFTER == rowKind) {
                    pstmt = setPstmtParams(value, upsertPstmt, false);
                } else if (RowKind.DELETE == rowKind) {
                    pstmt = setPstmtParams(value, delPstmt, true);
                }
                if (pstmt != null) pstmt.executeUpdate();
                break;
            } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }

                try {
                    if (!dbConn.isValid(CONNECTION_CHECK_TIMEOUT_SECONDS)) {
                        upsertPstmt.close();
                        delPstmt.close();
                        dbConn.close();
                        establishConnectionAndStatement();
                    }
                } catch (SQLException | ClassNotFoundException exception) {
                    LOG.error("JDBC connection is not valid, and reestablish connection failed", exception);
                    throw new RuntimeException("Reestablish JDBC connection failed", exception);
                }

                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    /**
     * @param rowData
     * @param KeyFilter
     * @return
     */
    private List<Object> getValues(RowData rowData, boolean KeyFilter) {
        IntStream stream = null;
        if (KeyFilter) { // 过滤主键
            List<Integer> keyList = IntStream.range(0, keyIndices.length).mapToObj(x -> x).collect(Collectors.toList());
            stream = IntStream.range(0, fieldDataTypes.length)
                    .filter(x -> keyList.contains(x));
        } else {
            stream = IntStream.range(0, fieldDataTypes.length);
        }

        return stream
                .mapToObj(x -> RowData.createFieldGetter(fieldDataTypes[x].getLogicalType(), x))
                .map(e -> e.getFieldOrNull(rowData))
                .map(v -> {
                    Object o = v;
                    if (v instanceof DecimalData) {
                        o = ((DecimalData) v).toBigDecimal();
                    } else if (v instanceof StringData) {
                        o = new String(((StringData) v).toBytes());
                    }
                    return o;
                }).collect(Collectors.toList());
    }

    /**
     * @param value
     * @throws SQLException
     */
    private PreparedStatement setPstmtParams(RowData value, PreparedStatement pstmt, boolean KeyFilter) throws SQLException {
        pstmt.clearParameters();
        List<Object> values = getValues(value, KeyFilter);
        for (int i = 0; i < values.size(); i++) {
            pstmt.setObject(i + 1, values.get(i));
        }
        return pstmt;
    }

    private void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Class.forName(Constans.PHOENIX_DRIVER); // 加载驱动
        dbConn = DriverManager.getConnection(String.format(Constans.PHOENIX_JDBC_URL_TEMPLATE, phoenixOptions.getServerUrl()));
        dbConn.setAutoCommit(true);
        String upsetSQL = buildUpsetSQL(phoenixOptions, fieldNames);
        LOG.info("Upsert SQL: {}", upsetSQL);
        upsertPstmt = dbConn.prepareStatement(upsetSQL);

        String delSQL = buildDelSQL(phoenixOptions, fieldNames, keyIndices);
        LOG.info("Delete SQL: {}", delSQL);
        delPstmt = dbConn.prepareStatement(delSQL);
    }


    @Override
    public void close() {
        LOG.info("start close ...");
        if (upsertPstmt != null) {
            try {
                upsertPstmt.close();
            } catch (Exception e) {
                LOG.warn("Exception occurs while closing PreparedStatement.", e);
            }
            this.upsertPstmt = null;
        }

        if (delPstmt != null) {
            try {
                delPstmt.close();
            } catch (Exception e) {
                LOG.warn("Exception occurs while closing PreparedStatement.", e);
            }
            this.delPstmt = null;
        }

        if (dbConn != null) {
            try {
                dbConn.close();
            } catch (Exception e) {
                LOG.warn("Exception occurs while closing Phoenix Connection.", e);
            }
            this.dbConn = null;
        }
        LOG.info("end close.");
    }
}
