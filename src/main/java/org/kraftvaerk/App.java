package org.kraftvaerk;

import com.amazonaws.services.qldb.AmazonQLDB;
import com.amazonaws.services.qldb.AmazonQLDBClientBuilder;
import com.amazonaws.services.qldb.model.*;
import com.amazonaws.services.qldbsession.AmazonQLDBSessionClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.PooledQldbDriver;
import software.amazon.qldb.QldbSession;
import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;


import java.time.Duration;

public class App {
    public static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        String ledgerName = "MyLedger";
        String tableName = "MyTable";
        try {
            log.info("Creating ledger");
            AmazonQLDB qldbClient = AmazonQLDBClientBuilder.standard().build();
            createLedger(ledgerName, qldbClient);

            PooledQldbDriver pooledQlDbDriver = getPooledDriver(ledgerName);

            log.info("Creating tables");
            createTables(pooledQlDbDriver, tableName);

            log.info("Inserting");
            insertDummyData(tableName, pooledQlDbDriver);

            log.info("Fetching data back out");
            queryTable(pooledQlDbDriver, tableName);

        } catch (Exception e) {
            log.error("error", e);
        }

    }

    private static PooledQldbDriver getPooledDriver(String ledgerName) {
        AmazonQLDBSessionClientBuilder builder = AmazonQLDBSessionClientBuilder.standard();
        
        return PooledQldbDriver.builder()
                .withSessionClientBuilder(builder)
                .withLedger(ledgerName)
                .withRetryLimit(3)
                .build();
    }

    private static void createLedger(String ledgerName,AmazonQLDB qldb) throws InterruptedException {

        CreateLedgerRequest createLedgerRequest = new CreateLedgerRequest()
                .withName(ledgerName)
                .withPermissionsMode(PermissionsMode.ALLOW_ALL);


        CreateLedgerResult ledger = qldb.createLedger(createLedgerRequest);
        Boolean created = false;
        log.info("Waiting for ledger to become active...");
        while (!created) {
            DescribeLedgerResult result = describe(ledgerName,qldb);
            if (result.getState().equals(LedgerState.ACTIVE.name())) {
                log.info("Success. Ledger is active and ready to use.");
                created = true;
            }
            log.info("The ledger is still creating. Please wait...");

            Thread.sleep(Duration.ofSeconds(15).toMillis());
        }
    }

    public static DescribeLedgerResult describe(final String name, AmazonQLDB qldb) {
        log.info("Let's describe ledger with name: {}...", name);
        DescribeLedgerRequest request = new DescribeLedgerRequest().withName(name);
        DescribeLedgerResult result = qldb.describeLedger(request);
        log.info("Success. Ledger description: {}", result);
        return result;
    }

    private static void queryTable(PooledQldbDriver driver, String tableName) {
        try (QldbSession qldbSession = driver.getSession()) {

            qldbSession.execute(txn -> {
                Result result = txn.execute(String.format("select * from %s", tableName));
                printDocuments(result);
            }, (retryAttempt) -> log.info("Retrying due to OCC conflict..."));
            log.info("Tables created successfully!");
        } catch (Exception e) {
            log.error("Errors creating tables.", e);
        }
    }

    public static void printDocuments(final Result result) {
        result.iterator().forEachRemaining(row -> log.info(row.toPrettyString()));
    }


    public static void createTables(PooledQldbDriver driver, String tableName) throws InterruptedException {
        try (QldbSession qldbSession = driver.getSession()) {
            qldbSession.execute(txn -> {
                createTable(txn, tableName);
            }, (retryAttempt) -> log.info("Retrying due to OCC conflict..."));
            log.info("Tables created successfully!");
        } catch (Exception e) {
            log.error("Errors creating tables.", e);
        }
        Thread.sleep(5000);
    }

    public static void createTable(final TransactionExecutor txn, final String tableName) {
        log.info("Creating the '{}' table...", tableName);
        final String createTable = String.format("CREATE TABLE %s", tableName);
        final Result result = txn.execute(createTable);
        log.info("{} table created successfully.", tableName);
    }

    public static void insertDummyData(String tableName, PooledQldbDriver driver) {
        try (QldbSession qldbSession = driver.getSession()) {

            qldbSession.execute(txn -> {
                String document = String.format("{ 'mytabledate': %s}", System.currentTimeMillis());
                final String insertValue = String.format("INSERT INTO %s value %s", tableName, document);
                txn.execute(insertValue);
            }, (retryAttempt) -> log.info("Retrying due to OCC conflict..."));
            log.info("Tables created successfully!");
        } catch (Exception e) {
            log.error("Errors creating tables.", e);
        }
    }

}
