package com.safariyetu.commons.bigqueryobjects;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.safariyetu.commons.bigqueryobjects.BigQueryObjectWriter;

@ExtendWith(MockitoExtension.class)
public class BigQueryWriterTest {

    @Mock
    private BigQuery bigquery;

    private BigQueryObjectWriter bigQueryHelper;

    @BeforeEach
    public void setUp() {
        bigQueryHelper = new BigQueryObjectWriter(bigquery);
    }

    // Test data classes
    public static class TestSimpleObject {
        private String name;
        private int age;
        private boolean active;
        private double score;

        public TestSimpleObject(String name, int age, boolean active, double score) {
            this.name = name;
            this.age = age;
            this.active = active;
            this.score = score;
        }
    }

    public static class TestComplexObject {
        private String id;
        private LocalDate date;
        private Instant timestamp;
        private BigDecimal amount;
        private TestSimpleObject nested;

        public TestComplexObject(String id, LocalDate date, Instant timestamp, BigDecimal amount, TestSimpleObject nested) {
            this.id = id;
            this.date = date;
            this.timestamp = timestamp;
            this.amount = amount;
            this.nested = nested;
        }
    }

    public static class TestCollectionObject {
        private String category;
        private List<String> tags;
        private List<TestSimpleObject> items;

        public TestCollectionObject(String category, List<String> tags, List<TestSimpleObject> items) {
            this.category = category;
            this.tags = tags;
            this.items = items;
        }
    }

    @Test
    public void testExecute_whenRowsAreEmpty_thenDoNothing() {
        // Execute
        bigQueryHelper.insert("test_dataset", "test_table").execute();

        // Verify - no interactions with BigQuery
        verify(bigquery, never()).insertAll(any(InsertAllRequest.class));
    }

    @Test
    public void testExecute_whenInsertingSingleRow_thenInsertSuccessfully() {
        // Setup
        TestSimpleObject testObject = new TestSimpleObject("John", 30, true, 95.5);
        
        InsertAllResponse response = mockInsertAllResponse(false);
        when(bigquery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        // Execute
        bigQueryHelper.insert("test_dataset", "test_table")
                .row(testObject)
                .execute();

        // Verify
        verify(bigquery).insertAll(any(InsertAllRequest.class));
    }

    @Test
    public void testExecute_whenInsertingMultipleRows_thenInsertSuccessfully() {
        // Setup
        List<TestSimpleObject> testObjects = Arrays.asList(
            new TestSimpleObject("John", 30, true, 95.5),
            new TestSimpleObject("Jane", 25, false, 88.2)
        );

        InsertAllResponse response = mockInsertAllResponse(false);
        when(bigquery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        // Execute
        bigQueryHelper.insert("test_dataset", "test_table")
                .rows(testObjects)
                .execute();

        // Verify
        verify(bigquery).insertAll(any(InsertAllRequest.class));
    }

    @Test
    public void testExecute_whenInsertHasErrors_thenThrowRuntimeException() {
        // Setup
        TestSimpleObject testObject = new TestSimpleObject("John", 30, true, 95.5);

        InsertAllResponse response = mockInsertAllResponse(true);
        Map<Long, List<BigQueryError>> errors = new HashMap<>();
        errors.put(0L, Arrays.asList(new BigQueryError("reason", "location", "message")));
        when(response.getInsertErrors()).thenReturn(errors);
        
        when(bigquery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        // Execute and Verify
        Assertions.assertThrows(RuntimeException.class, 
        		() -> 
            bigQueryHelper.insert("test_dataset", "test_table")
                    .row(testObject)
                    .execute()
        );

        verify(bigquery).insertAll(any(InsertAllRequest.class));
    }

    @Test
    public void testExecute_whenTableNotFound_thenCreateTableAndRetrySuccess() {
        // Setup
        TestSimpleObject testObject = new TestSimpleObject("John", 30, true, 95.5);
        TableId tableId = TableId.of("test_dataset", "test_table");

        // First call fails with notFound error
        BigQueryError error = new BigQueryError("notFound", "", "");
        BigQueryException notFoundException = new BigQueryException(404, "Table not found", error);
        when(bigquery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(notFoundException)
                .thenReturn(mockInsertAllResponse(false));

        // Table doesn't exist
        when(bigquery.getTable(tableId)).thenReturn(null);

        // Create table
        Table createdTable = createMockTable();
        when(bigquery.create(any(TableInfo.class))).thenReturn(createdTable);

        // Execute
        bigQueryHelper.insert("test_dataset", "test_table")
                .row(testObject)
                .execute();

        // Verify
        verify(bigquery, times(2)).insertAll(any(InsertAllRequest.class));
        verify(bigquery).getTable(tableId);
        verify(bigquery).create(any(TableInfo.class));
    }

    @Test
    public void testExecute_whenSchemaMismatch_thenUpdateTableAndRetrySuccess() {
        // Setup
        TestSimpleObject testObject = new TestSimpleObject("John", 30, true, 95.5);
        TableId tableId = TableId.of("test_dataset", "test_table");

        // First call fails with schema mismatch error
        BigQueryError error = new BigQueryError("invalid", "", "");
        BigQueryException schemaMismatchException = new BigQueryException(400, "schema mismatch", error);
        when(bigquery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(schemaMismatchException)
                .thenReturn(mockInsertAllResponse(false));

        // Table exists with different schema
        Table existingTable = createMockTable();
        when(bigquery.getTable(tableId)).thenReturn(existingTable);

        StandardTableDefinition tableDefinition = createMockTableDefinition();
        when(existingTable.getDefinition()).thenReturn(tableDefinition);

        // Create a real Schema instance for the current schema
        Schema currentSchema = Schema.of(
            Field.of("different_field", LegacySQLTypeName.STRING),
            Field.of("another_field", LegacySQLTypeName.INTEGER)
        );
        when(tableDefinition.getSchema()).thenReturn(currentSchema);

        // Mock that the table has no partitioning or clustering (null)
        //when(tableDefinition.getTimePartitioning()).thenReturn(null);
        //when(tableDefinition.getClustering()).thenReturn(null);

        // Mock the table builder chain - FIX: Make setDefinition return the builder
        Table.Builder tableBuilder = createMockTableBuilder();
        when(existingTable.toBuilder()).thenReturn(tableBuilder);
        
        // FIX: Make setDefinition return the builder for method chaining
        when(tableBuilder.setDefinition(any(StandardTableDefinition.class))).thenReturn(tableBuilder);
        
        Table updatedTable = createMockTable();
        when(tableBuilder.build()).thenReturn(updatedTable);
        when(bigquery.update(updatedTable)).thenReturn(updatedTable);

        // Execute
        bigQueryHelper.insert("test_dataset", "test_table")
                .row(testObject)
                .execute();

        // Verify
        verify(bigquery, times(2)).insertAll(any(InsertAllRequest.class));
        verify(bigquery).getTable(tableId);
        verify(bigquery).update(updatedTable);
    }

    @Test
    public void testObjectToMap_whenObjectHasComplexTypes_thenCorrectlyMapFields() {
        // Setup
        TestSimpleObject nested = new TestSimpleObject("Nested", 10, false, 50.0);
        TestComplexObject complexObject = new TestComplexObject(
            "test-id",
            LocalDate.of(2023, 12, 25),
            Instant.parse("2023-12-25T10:15:30.00Z"),
            new BigDecimal("123.45"),
            nested
        );

        // Execute
        Map<String, Object> result = bigQueryHelper.objectToMap(complexObject);

        // Verify using Google Truth
        assertThat(result).containsKey("id");
        assertThat(result.get("id")).isEqualTo("test-id");
        assertThat(result.get("date")).isEqualTo("2023-12-25");
        assertThat(result.get("timestamp")).isEqualTo("2023-12-25T10:15:30Z");
        assertThat(result.get("amount")).isEqualTo("123.45");

        assertThat(result.get("nested")).isInstanceOf(Map.class);
        Map<?, ?> nestedMap = (Map<?, ?>) result.get("nested");
        assertThat(nestedMap.get("name")).isEqualTo("Nested");
        assertThat(nestedMap.get("age")).isEqualTo(10);
    }

    @Test
    public void testObjectToMap_whenObjectHasCollections_thenCorrectlyMapRepeatedFields() {
        // Setup
        List<String> tags = Arrays.asList("tag1", "tag2", "tag3");
        List<TestSimpleObject> items = Arrays.asList(
            new TestSimpleObject("Item1", 1, true, 10.0),
            new TestSimpleObject("Item2", 2, false, 20.0)
        );

        TestCollectionObject collectionObject = new TestCollectionObject("test-category", tags, items);

        // Execute
        Map<String, Object> result = bigQueryHelper.objectToMap(collectionObject);

        // Verify
        assertThat(result.get("category")).isEqualTo("test-category");
        assertThat(result.get("tags")).isInstanceOf(List.class);
        assertThat((List<?>) result.get("tags")).containsExactly("tag1", "tag2", "tag3");

        assertThat(result.get("items")).isInstanceOf(List.class);
        List<?> itemsList = (List<?>) result.get("items");
        assertThat(itemsList).hasSize(2);
        assertThat(itemsList.get(0)).isInstanceOf(Map.class);
    }

    @Test
    public void testGetSchemaFromClass_whenSimpleObject_thenCorrectlyMapFields() {
        // Execute
        List<com.google.cloud.bigquery.Field> fields = bigQueryHelper.getFieldsFromClass(TestSimpleObject.class);

        // Verify
        assertThat(fields).hasSize(4);
        
        Map<String, LegacySQLTypeName> fieldTypes = new HashMap<>();
        for (com.google.cloud.bigquery.Field field : fields) {
            fieldTypes.put(field.getName(), field.getType());
        }

        assertThat(fieldTypes.get("name")).isEqualTo(LegacySQLTypeName.STRING);
        assertThat(fieldTypes.get("age")).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(fieldTypes.get("active")).isEqualTo(LegacySQLTypeName.BOOLEAN);
        assertThat(fieldTypes.get("score")).isEqualTo(LegacySQLTypeName.FLOAT);
    }

    @Test
    public void testGetSchemaFromClass_whenNestedObject_thenCorrectlyMapRecords() {
        // Execute
        List<com.google.cloud.bigquery.Field> fields = bigQueryHelper.getFieldsFromClass(TestComplexObject.class);

        // Verify
        assertThat(fields).hasSize(5);
        
        // Find the nested field
        com.google.cloud.bigquery.Field nestedField = fields.stream()
                .filter(f -> f.getName().equals("nested"))
                .findFirst()
                .orElseThrow();

        assertThat(nestedField.getType()).isEqualTo(LegacySQLTypeName.RECORD);
        assertThat(nestedField.getSubFields()).isNotNull();
        assertThat(nestedField.getSubFields()).hasSize(4); // Fields from TestSimpleObject
    }

    @Test
    public void testGetSchemaFromObjects_whenEmptyList_thenThrowIllegalArgumentException() {
        // Execute and Verify
    	 Assertions.assertThrows(IllegalArgumentException.class, 
    			 () -> bigQueryHelper.getSchemaFromObjects(new ArrayList<>()))
            ;
    }

    @Test
    public void testGetTypeFromClass_whenAllSupportedTypes_thenReturnCorrectLegacySQLType() {
        // Test primitive and wrapper types
        assertThat(bigQueryHelper.getTypeFromClass(int.class)).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(bigQueryHelper.getTypeFromClass(Integer.class)).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(bigQueryHelper.getTypeFromClass(String.class)).isEqualTo(LegacySQLTypeName.STRING);
        assertThat(bigQueryHelper.getTypeFromClass(boolean.class)).isEqualTo(LegacySQLTypeName.BOOLEAN);
        assertThat(bigQueryHelper.getTypeFromClass(Boolean.class)).isEqualTo(LegacySQLTypeName.BOOLEAN);
        assertThat(bigQueryHelper.getTypeFromClass(double.class)).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(bigQueryHelper.getTypeFromClass(Double.class)).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(bigQueryHelper.getTypeFromClass(BigDecimal.class)).isEqualTo(LegacySQLTypeName.NUMERIC);
        assertThat(bigQueryHelper.getTypeFromClass(LocalDate.class)).isEqualTo(LegacySQLTypeName.DATE);
        assertThat(bigQueryHelper.getTypeFromClass(Instant.class)).isEqualTo(LegacySQLTypeName.TIMESTAMP);
        assertThat(bigQueryHelper.getTypeFromClass(Date.class)).isEqualTo(LegacySQLTypeName.TIMESTAMP);

        // Test unsupported types return null
        assertThat(bigQueryHelper.getTypeFromClass(Object.class)).isNull();
        assertThat(bigQueryHelper.getTypeFromClass(List.class)).isNull();
    }

    @Test
    public void testExecute_whenTableNotFoundByReason_thenCreateTableAndRetrySuccess() {
        // Setup
        TestSimpleObject testObject = new TestSimpleObject("John", 30, true, 95.5);
        TableId tableId = TableId.of("test_dataset", "test_table");

        // Create a BigQueryError with "notFound" reason and use it to create BigQueryException
        BigQueryError notFoundError = new BigQueryError("notFound", "location", "Table not found");
        BigQueryException notFoundException = new BigQueryException(404, "Table not found", notFoundError);
        
        when(bigquery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(notFoundException)
                .thenReturn(mockInsertAllResponse(false));

        // Table doesn't exist
        when(bigquery.getTable(tableId)).thenReturn(null);

        // Create table
        Table createdTable = createMockTable();
        when(bigquery.create(any(TableInfo.class))).thenReturn(createdTable);

        // Execute
        bigQueryHelper.insert("test_dataset", "test_table")
                .row(testObject)
                .execute();

        // Verify
        verify(bigquery, times(2)).insertAll(any(InsertAllRequest.class));
        verify(bigquery).getTable(tableId);
        verify(bigquery).create(any(TableInfo.class));
    }

    @Test
    public void testExecute_whenSchemaMismatchByMessage_thenUpdateTableAndRetrySuccess() {
        // Setup
        TestSimpleObject testObject = new TestSimpleObject("John", 30, true, 95.5);
        TableId tableId = TableId.of("test_dataset", "test_table");

        // Create exception with message containing "schema mismatch"
        BigQueryException schemaMismatchException = new BigQueryException(400, "schema mismatch: Field 'name' has type STRING but expected INTEGER");
        when(bigquery.insertAll(any(InsertAllRequest.class)))
                .thenThrow(schemaMismatchException)
                .thenReturn(mockInsertAllResponse(false));

        // Table exists with different schema
        Table existingTable = createMockTable();
        when(bigquery.getTable(tableId)).thenReturn(existingTable);

        StandardTableDefinition tableDefinition = createMockTableDefinition();
        when(existingTable.getDefinition()).thenReturn(tableDefinition);

        // Create a real Schema instance for the current schema
        Schema currentSchema = Schema.of(
            Field.of("different_field", LegacySQLTypeName.STRING),
            Field.of("another_field", LegacySQLTypeName.INTEGER)
        );
        when(tableDefinition.getSchema()).thenReturn(currentSchema);

        // Mock that the table has no partitioning or clustering (null)
        //when(tableDefinition.getTimePartitioning()).thenReturn(null);
        //when(tableDefinition.getClustering()).thenReturn(null);

        // Mock the table builder chain - FIX: Make setDefinition return the builder
        Table.Builder tableBuilder = createMockTableBuilder();
        when(existingTable.toBuilder()).thenReturn(tableBuilder);
        
        // FIX: Make setDefinition return the builder for method chaining
        when(tableBuilder.setDefinition(any(StandardTableDefinition.class))).thenReturn(tableBuilder);
        
        Table updatedTable = createMockTable();
        when(tableBuilder.build()).thenReturn(updatedTable);
        when(bigquery.update(updatedTable)).thenReturn(updatedTable);

        // Execute
        bigQueryHelper.insert("test_dataset", "test_table")
                .row(testObject)
                .execute();

        // Verify
        verify(bigquery, times(2)).insertAll(any(InsertAllRequest.class));
        verify(bigquery).getTable(tableId);
        verify(bigquery).update(updatedTable);
    }

    @Test
    public void testObjectToMap_whenAllSupportedTypes_thenCorrectlyConvertValues() {
        // Setup - test class with all supported types
        class TestAllSupportedTypes {
            private String stringField;
            private int intField;
            private Integer integerField;
            private long longField;
            private Long longWrapperField;
            private boolean booleanField;
            private Boolean booleanWrapperField;
            private float floatField;
            private Float floatWrapperField;
            private double doubleField;
            private Double doubleWrapperField;
            private BigDecimal bigDecimalField;
            private LocalDate localDateField;
            private LocalTime localTimeField;
            private LocalDateTime localDateTimeField;
            private Instant instantField;
            private Date dateField;
            private java.sql.Date sqlDateField;

            public TestAllSupportedTypes(String stringField, int intField, Integer integerField, 
                                       long longField, Long longWrapperField, boolean booleanField, 
                                       Boolean booleanWrapperField, float floatField, Float floatWrapperField,
                                       double doubleField, Double doubleWrapperField, BigDecimal bigDecimalField,
                                       LocalDate localDateField, LocalTime localTimeField, 
                                       LocalDateTime localDateTimeField, Instant instantField, Date dateField,
                                       java.sql.Date sqlDateField) {
                this.stringField = stringField;
                this.intField = intField;
                this.integerField = integerField;
                this.longField = longField;
                this.longWrapperField = longWrapperField;
                this.booleanField = booleanField;
                this.booleanWrapperField = booleanWrapperField;
                this.floatField = floatField;
                this.floatWrapperField = floatWrapperField;
                this.doubleField = doubleField;
                this.doubleWrapperField = doubleWrapperField;
                this.bigDecimalField = bigDecimalField;
                this.localDateField = localDateField;
                this.localTimeField = localTimeField;
                this.localDateTimeField = localDateTimeField;
                this.instantField = instantField;
                this.dateField = dateField;
                this.sqlDateField = sqlDateField;
            }
        }

        LocalDate testDate = LocalDate.of(2023, 12, 25);
        LocalTime testTime = LocalTime.of(10, 15, 30);
        LocalDateTime testDateTime = LocalDateTime.of(testDate, testTime);
        Instant testInstant = Instant.parse("2023-12-25T10:15:30.00Z");
        Date testUtilDate = Date.from(testInstant);
        java.sql.Date sqlDateField = new java.sql.Date(testInstant.plusMillis(1000).toEpochMilli());
        BigDecimal testBigDecimal = new BigDecimal("123.456789");

        TestAllSupportedTypes testObject = new TestAllSupportedTypes(
            "test", 42, 100, 999L, 888L, true, false, 
            1.5f, 2.7f, 3.14, 2.71, testBigDecimal,
            testDate, testTime, testDateTime, testInstant, testUtilDate,
            sqlDateField
        );

        // Execute
        Map<String, Object> result = bigQueryHelper.objectToMap(testObject);

        // Verify
        assertThat(result.get("stringField")).isEqualTo("test");
        assertThat(result.get("intField")).isEqualTo(42);
        assertThat(result.get("integerField")).isEqualTo(100);
        assertThat(result.get("longField")).isEqualTo(999L);
        assertThat(result.get("longWrapperField")).isEqualTo(888L);
        assertThat(result.get("booleanField")).isEqualTo(true);
        assertThat(result.get("booleanWrapperField")).isEqualTo(false);
        assertThat(result.get("floatField")).isEqualTo(1.5f);
        assertThat(result.get("floatWrapperField")).isEqualTo(2.7f);
        assertThat(result.get("doubleField")).isEqualTo(3.14);
        assertThat(result.get("doubleWrapperField")).isEqualTo(2.71);
        assertThat(result.get("bigDecimalField")).isEqualTo("123.456789");
        assertThat(result.get("localDateField")).isEqualTo("2023-12-25");
        assertThat(result.get("localTimeField")).isEqualTo("10:15:30");
        assertThat(result.get("localDateTimeField")).isEqualTo("2023-12-25T10:15:30");
        assertThat(result.get("instantField")).isEqualTo("2023-12-25T10:15:30Z");
        assertThat(result.get("dateField")).isInstanceOf(String.class); // Should be converted to string
        assertThat(result.get("sqlDateField")).isInstanceOf(String.class); // Should be converted to string
    }

    @Test
    public void testGetSchemaFromClass_whenAllSupportedTypes_thenCorrectlyMapFields() {
        // Test class with all supported types
        class TestAllSupportedTypes {
            private String stringField;
            private int intField;
            private Integer integerField;
            private long longField;
            private Long longWrapperField;
            private boolean booleanField;
            private Boolean booleanWrapperField;
            private float floatField;
            private Float floatWrapperField;
            private double doubleField;
            private Double doubleWrapperField;
            private BigDecimal bigDecimalField;
            private LocalDate localDateField;
            private LocalTime localTimeField;
            private LocalDateTime localDateTimeField;
            private Instant instantField;
            private Date dateField;
            private java.sql.Date sqlDateField;
        }

        // Execute
        List<com.google.cloud.bigquery.Field> fields = bigQueryHelper.getFieldsFromClass(TestAllSupportedTypes.class);

        // Verify
        Map<String, LegacySQLTypeName> fieldTypes = new HashMap<>();
        for (com.google.cloud.bigquery.Field field : fields) {
            fieldTypes.put(field.getName(), field.getType());
        }

        assertThat(fieldTypes.get("stringField")).isEqualTo(LegacySQLTypeName.STRING);
        assertThat(fieldTypes.get("intField")).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(fieldTypes.get("integerField")).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(fieldTypes.get("longField")).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(fieldTypes.get("longWrapperField")).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(fieldTypes.get("booleanField")).isEqualTo(LegacySQLTypeName.BOOLEAN);
        assertThat(fieldTypes.get("booleanWrapperField")).isEqualTo(LegacySQLTypeName.BOOLEAN);
        assertThat(fieldTypes.get("floatField")).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(fieldTypes.get("floatWrapperField")).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(fieldTypes.get("doubleField")).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(fieldTypes.get("doubleWrapperField")).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(fieldTypes.get("bigDecimalField")).isEqualTo(LegacySQLTypeName.NUMERIC);
        assertThat(fieldTypes.get("localDateField")).isEqualTo(LegacySQLTypeName.DATE);
        assertThat(fieldTypes.get("localTimeField")).isEqualTo(LegacySQLTypeName.TIME);
        assertThat(fieldTypes.get("localDateTimeField")).isEqualTo(LegacySQLTypeName.DATETIME);
        assertThat(fieldTypes.get("instantField")).isEqualTo(LegacySQLTypeName.TIMESTAMP);
        assertThat(fieldTypes.get("dateField")).isEqualTo(LegacySQLTypeName.TIMESTAMP);
        assertThat(fieldTypes.get("sqlDateField")).isEqualTo(LegacySQLTypeName.TIMESTAMP);
    }

    @Test
    public void testGetTypeFromClass_whenAllSupportedTypes_thenCorrectlyReturnLegacySQLType() {
        // Test all supported types
        assertThat(bigQueryHelper.getTypeFromClass(String.class)).isEqualTo(LegacySQLTypeName.STRING);
        assertThat(bigQueryHelper.getTypeFromClass(int.class)).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(bigQueryHelper.getTypeFromClass(Integer.class)).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(bigQueryHelper.getTypeFromClass(long.class)).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(bigQueryHelper.getTypeFromClass(Long.class)).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(bigQueryHelper.getTypeFromClass(boolean.class)).isEqualTo(LegacySQLTypeName.BOOLEAN);
        assertThat(bigQueryHelper.getTypeFromClass(Boolean.class)).isEqualTo(LegacySQLTypeName.BOOLEAN);
        assertThat(bigQueryHelper.getTypeFromClass(float.class)).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(bigQueryHelper.getTypeFromClass(Float.class)).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(bigQueryHelper.getTypeFromClass(double.class)).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(bigQueryHelper.getTypeFromClass(Double.class)).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(bigQueryHelper.getTypeFromClass(BigDecimal.class)).isEqualTo(LegacySQLTypeName.NUMERIC);
        assertThat(bigQueryHelper.getTypeFromClass(LocalDate.class)).isEqualTo(LegacySQLTypeName.DATE);
        assertThat(bigQueryHelper.getTypeFromClass(LocalTime.class)).isEqualTo(LegacySQLTypeName.TIME);
        assertThat(bigQueryHelper.getTypeFromClass(LocalDateTime.class)).isEqualTo(LegacySQLTypeName.DATETIME);
        assertThat(bigQueryHelper.getTypeFromClass(Instant.class)).isEqualTo(LegacySQLTypeName.TIMESTAMP);
        assertThat(bigQueryHelper.getTypeFromClass(Date.class)).isEqualTo(LegacySQLTypeName.TIMESTAMP);
        assertThat(bigQueryHelper.getTypeFromClass(java.sql.Date.class)).isEqualTo(LegacySQLTypeName.TIMESTAMP);

        // Test some unsupported types
        assertThat(bigQueryHelper.getTypeFromClass(Object.class)).isNull();
        assertThat(bigQueryHelper.getTypeFromClass(List.class)).isNull();
        assertThat(bigQueryHelper.getTypeFromClass(Map.class)).isNull();
    }

    @Test
    public void testExecute_whenTemporalTypesAreUsed_thenInsertSuccessfully() {
        // Setup - test class with LocalTime and other temporal types
        class TestTemporalTypes {
            private LocalTime localTimeField;
            private LocalDate localDateField;
            private LocalDateTime localDateTimeField;
            private Instant instantField;

            public TestTemporalTypes(LocalTime localTimeField, LocalDate localDateField, 
                                   LocalDateTime localDateTimeField, Instant instantField) {
                this.localTimeField = localTimeField;
                this.localDateField = localDateField;
                this.localDateTimeField = localDateTimeField;
                this.instantField = instantField;
            }
        }

        LocalTime testTime = LocalTime.of(14, 30, 45);
        LocalDate testDate = LocalDate.of(2023, 12, 25);
        LocalDateTime testDateTime = LocalDateTime.of(testDate, testTime);
        Instant testInstant = Instant.parse("2023-12-25T14:30:45.00Z");

        TestTemporalTypes testObject = new TestTemporalTypes(testTime, testDate, testDateTime, testInstant);

        InsertAllResponse response = mockInsertAllResponse(false);
        when(bigquery.insertAll(any(InsertAllRequest.class))).thenReturn(response);

        // Execute
        bigQueryHelper.insert("test_dataset", "test_table")
                .row(testObject)
                .execute();

        // Verify
        verify(bigquery).insertAll(any(InsertAllRequest.class));
    }

    @Test
    public void testObjectToMap_whenValuesAreNull_thenExcludeFieldsFromMap() {
        // Setup - test class with null values
        class TestWithNulls {
            private String stringField;
            private Integer integerField;
            private LocalDate localDateField;
            private BigDecimal bigDecimalField;

            public TestWithNulls(String stringField, Integer integerField, 
                               LocalDate localDateField, BigDecimal bigDecimalField) {
                this.stringField = stringField;
                this.integerField = integerField;
                this.localDateField = localDateField;
                this.bigDecimalField = bigDecimalField;
            }
        }

        TestWithNulls testObject = new TestWithNulls(null, null, null, null);

        // Execute
        Map<String, Object> result = bigQueryHelper.objectToMap(testObject);

        // Verify - null values should be excluded from the map
        assertThat(result).doesNotContainKey("stringField");
        assertThat(result).doesNotContainKey("integerField");
        assertThat(result).doesNotContainKey("localDateField");
        assertThat(result).doesNotContainKey("bigDecimalField");
        assertThat(result).isEmpty();
    }
    
    @Test
    public void testGetSchemaFromClass_whenCollectionOfPrimitives_thenCreateRepeatedFields() {
        // Test class with collections of primitive types
        class TestCollectionPrimitives {
            private List<String> stringList;
            private Set<Integer> integerSet;
            private Collection<Boolean> booleanCollection;
            private List<Double> doubleList;

            public TestCollectionPrimitives(List<String> stringList, Set<Integer> integerSet, 
                                          Collection<Boolean> booleanCollection, List<Double> doubleList) {
                this.stringList = stringList;
                this.integerSet = integerSet;
                this.booleanCollection = booleanCollection;
                this.doubleList = doubleList;
            }
        }

        // Execute
        List<com.google.cloud.bigquery.Field> fields = bigQueryHelper.getFieldsFromClass(TestCollectionPrimitives.class);

        // Verify
        assertThat(fields).hasSize(4);
        
        Map<String, LegacySQLTypeName> fieldTypes = new HashMap<>();
        Map<String, com.google.cloud.bigquery.Field.Mode> fieldModes = new HashMap<>();
        for (com.google.cloud.bigquery.Field field : fields) {
            fieldTypes.put(field.getName(), field.getType());
            fieldModes.put(field.getName(), field.getMode());
        }

        assertThat(fieldTypes.get("stringList")).isEqualTo(LegacySQLTypeName.STRING);
        assertThat(fieldModes.get("stringList")).isEqualTo(com.google.cloud.bigquery.Field.Mode.REPEATED);
        
        assertThat(fieldTypes.get("integerSet")).isEqualTo(LegacySQLTypeName.INTEGER);
        assertThat(fieldModes.get("integerSet")).isEqualTo(com.google.cloud.bigquery.Field.Mode.REPEATED);
        
        assertThat(fieldTypes.get("booleanCollection")).isEqualTo(LegacySQLTypeName.BOOLEAN);
        assertThat(fieldModes.get("booleanCollection")).isEqualTo(com.google.cloud.bigquery.Field.Mode.REPEATED);
        
        assertThat(fieldTypes.get("doubleList")).isEqualTo(LegacySQLTypeName.FLOAT);
        assertThat(fieldModes.get("doubleList")).isEqualTo(com.google.cloud.bigquery.Field.Mode.REPEATED);
    }

    // Helper methods for creating mocks
    private InsertAllResponse mockInsertAllResponse(boolean hasErrors) {
        InsertAllResponse response = org.mockito.Mockito.mock(InsertAllResponse.class);
        when(response.hasErrors()).thenReturn(hasErrors);
        return response;
    }

    private Table createMockTable() {
        return org.mockito.Mockito.mock(Table.class);
    }

    private StandardTableDefinition createMockTableDefinition() {
        return org.mockito.Mockito.mock(StandardTableDefinition.class);
    }

    private Table.Builder createMockTableBuilder() {
        Table.Builder tableBuilder = org.mockito.Mockito.mock(Table.Builder.class);
        // Ensure setDefinition returns the builder for method chaining
        //when(tableBuilder.setDefinition(any(TableDefinition.class))).thenReturn(tableBuilder);
        return tableBuilder;
    }
    
}