package org.example;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.util.concurrent.atomic.AtomicLong;

public class Main {

    private BoundStatement crdtIncrementStatement, crdtDecrementStatement, crdtSelectStatement, selectStatement,
            selectStatementForInitializeLocalValues,
            selectStatementForInitializeCRDTValues, selectOtherRegionLocalValues,
            selectOtherRegionCRDTValues, checkValueTableStatement = null;
    PreparedStatement updateSyncStartedStatement, crdtIncrementDecrementStatement, insertValueMapToLocal,
            insertCRDTvaluesToCRDTSpace,
            updateValueMapToCRDTSpace, updateValuesToLocalSpace, checkUpdateSyncStartedStatement,
            updateTheUpdateSyncStartedStatement, crdtResetStatement;
    private Session localSession;
    Cluster localCluster, crdtCluster = null;
    java.util.UUID uuid1;
    protected static final String VALUE_COLUMN_NAME = "valueColumn";
    protected static final String CRDT_DATABASE_SPACE_NAME = "crdt_keyspace";
    protected static final String COUNTER_COLLECTION_NAME = "counter_table";
    private static long durationInMillies;
    private static String hostname;
    private static AtomicLong latency = new AtomicLong(0L);
    private java.util.UUID uuiid;


    public static void main(String[] args) {
        if (args.length > 0) {
            durationInMillies = Long.parseLong(args[0]) * 60L * 1000L;
        } else {
            durationInMillies = 60000L;
        }
        if (args.length > 1) {
            hostname = args[1];
        } else {
            hostname = "34.45.179.107";
        }
        Main main = new Main();
        main.initCounter();
        System.out.println(main.getCounterValue());
//        main.incrementDecrementCounter(-78881L);
//        System.out.println(main.getCounterValue());
    }

    public void initCounter() {
        String uuid = "8093cd4f-f345-4b1f-b39f-b63810e507a8";
//        String uuid = "ce0b9e47-23e9-406e-a361-7956e2d191a7";
        uuid1 = java.util.UUID.fromString(uuid);
        PoolingOptions poolingOptions = new PoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 8)    // Maximum connections
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 10)
                .setCoreConnectionsPerHost(HostDistance.REMOTE, 0)   // No remote connections
                .setMaxConnectionsPerHost(HostDistance.REMOTE, 0)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768) // Maximum requests
                .setNewConnectionThreshold(HostDistance.LOCAL, 800);

        // Aggressive socket options for speed
        SocketOptions socketOptions = new SocketOptions()
                .setConnectTimeoutMillis(1000)    // Quick connection timeout
                .setReadTimeoutMillis(1000)       // Quick read timeout
                .setTcpNoDelay(true)              // Disable Nagle's algorithm
                .setKeepAlive(true)
                .setSoLinger(0)                   // Immediate close
                .setReuseAddress(true);

        localCluster = Cluster.builder()
                .addContactPoint(hostname)
                .withPort(9042)
                .withPoolingOptions(poolingOptions)
                .withSocketOptions(socketOptions)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .build();

        localSession = localCluster.connect();

//        String dropKeyspaceQuery = "DROP KEYSPACE IF EXISTS " + CRDT_DATABASE_SPACE_NAME + ";";
//        localSession.execute(dropKeyspaceQuery).getAvailableWithoutFetching();

        String query = "CREATE KEYSPACE IF NOT EXISTS " + CRDT_DATABASE_SPACE_NAME +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}";
        localSession.execute(query);
        query = "CREATE TABLE IF NOT EXISTS " + CRDT_DATABASE_SPACE_NAME + "." + COUNTER_COLLECTION_NAME +
                " (id UUID PRIMARY KEY, " + VALUE_COLUMN_NAME + " counter)";
        localSession.execute(query);
        crdtSelectStatement = localSession.prepare(
                "SELECT " + VALUE_COLUMN_NAME + " FROM " + CRDT_DATABASE_SPACE_NAME + "." +
                        COUNTER_COLLECTION_NAME + " WHERE id = ?").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
                .bind(uuid1);
        crdtIncrementDecrementStatement = localSession.prepare(
                "UPDATE " + CRDT_DATABASE_SPACE_NAME + "." + COUNTER_COLLECTION_NAME + " SET " +
                        VALUE_COLUMN_NAME + " = " + VALUE_COLUMN_NAME + " + ? " +
                        "WHERE id = ?").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
    }

    public long incrementDecrementCounter(long longValue) {
//        crdtSession.execute(crdtIncrementDecrementStatement.bind(longValue, uuid1));
        localSession.executeAsync(crdtIncrementDecrementStatement.bind(longValue, uuid1));
        return -1L;
//        return getCounterValue();
    }

    public long resetCounter(long resetCounterValue) {
        localSession.execute(crdtResetStatement.bind(resetCounterValue, uuid1));
        return getCounterValue();
    }

    public long getCounterValue() {
        ResultSet resultSet = localSession.execute(crdtSelectStatement);
        Row row = resultSet.one();
        if (row != null) {
            return row.getLong(VALUE_COLUMN_NAME);
        } else {
            return -1;
        }
    }
}