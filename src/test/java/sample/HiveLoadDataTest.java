package sample;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HiveLoadDataTest {

    @Test
    void buildsStatements() {
        assertEquals("CREATE DATABASE IF NOT EXISTS sales", HiveLoadData.createDatabaseSql("sales"));
        assertEquals("USE sales", HiveLoadData.useDatabaseSql("sales"));
        assertEquals("LOAD DATA LOCAL INPATH '/data/customers.txt' INTO TABLE customers_2024",
                HiveLoadData.loadDataSql("/data/customers.txt", "customers_2024"));
    }

    @Test
    void escapesQuotesInFilePath() {
        assertEquals("LOAD DATA LOCAL INPATH '/data/o\\'brien.txt' INTO TABLE t",
                HiveLoadData.loadDataSql("/data/o'brien.txt", "t"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "1abc", "a b", "x;DROP DATABASE y", "t`", "db.table"})
    void rejectsUnsafeIdentifiers(String name) {
        assertThrows(IllegalArgumentException.class, () -> HiveLoadData.identifier(name));
    }
}
