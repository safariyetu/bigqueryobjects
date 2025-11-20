package com.safariyetu.commons.bigqueryobjects;

import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import com.google.common.truth.Truth;
import com.safariyetu.commons.bigqueryobjects.BigQueryObjectReader;

@ExtendWith(MockitoExtension.class)
public class BigQueryObjectReaderTest {

    // Test POJO classes (same as before)
    public static class TestSimplePojo {
        private String name;
        private int age;
        private double score;
        private boolean active;
        public TestSimplePojo() {}
        @Override public String toString() {
            return "TestSimplePojo{name='" + name + "', age=" + age + ", score=" + score + ", active=" + active + '}';
        }
    }

    public static class TestComplexPojo {
        private String id;
        private LocalDate birthDate;
        private Instant createdTime;
        private BigDecimal amount;
        public TestComplexPojo() {}
        @Override public String toString() {
            return "TestComplexPojo{id='" + id + "', birthDate=" + birthDate + ", createdTime=" + createdTime + ", amount=" + amount + '}';
        }
    }

    public static class TestNestedPojo {
        private String name;
        private TestSimplePojo nested;
        public TestNestedPojo() {}
        @Override public String toString() {
            return "TestNestedPojo{name='" + name + "', nested=" + nested + '}';
        }
    }

    public static class TestCollectionPojo {
        private String category;
        private List<String> tags;
        private List<Integer> scores;
        public TestCollectionPojo() {}
        @Override public String toString() {
            return "TestCollectionPojo{category='" + category + "', tags=" + tags + ", scores=" + scores + '}';
        }
    }

    public static class TestDifferentFieldNamesPojo {
        private String identifier;
        private String fullName;
        private int years;
        public TestDifferentFieldNamesPojo() {}
        @Override public String toString() {
            return "TestDifferentFieldNamesPojo{identifier='" + identifier + "', fullName='" + fullName + "', years=" + years + '}';
        }
    }

    @Test
    public void of_whenCalledWithPojoClass_thenReturnsReaderInstance() {
        // Execute
        BigQueryObjectReader<TestSimplePojo> reader = BigQueryObjectReader.of(TestSimplePojo.class);

        // Verify
        Truth.assertThat(reader).isNotNull();
    }

    @Test
    public void mapTo_whenUsedInFluentChain_thenBuildsMappingCorrectly() {
        // Setup
        BigQueryObjectReader<TestDifferentFieldNamesPojo> reader = BigQueryObjectReader.of(TestDifferentFieldNamesPojo.class);

        // Execute
        reader.map("id").to("identifier")
               .map("name").to("fullName")
               .map("age").to("years");

        // Verify - no exception thrown means fluent API works
        Truth.assertThat(reader).isNotNull();
    }

    @Test
    public void to_whenCalledWithoutMapFirst_thenThrowsIllegalStateException() {
        // Setup
        BigQueryObjectReader<TestSimplePojo> reader = BigQueryObjectReader.of(TestSimplePojo.class);

        // Execute and Verify
        Assertions.assertThrows(IllegalStateException.class, 
        		() -> reader.to("fieldName"));
    }

    @Test
    public void read_whenSimpleTypes_thenMapsToPojoCorrectly(@Mock TableResult tableResult) {
        // Setup
        Schema schema = Schema.of(
            Field.of("name", LegacySQLTypeName.STRING),
            Field.of("age", LegacySQLTypeName.INTEGER),
            Field.of("score", LegacySQLTypeName.FLOAT),
            Field.of("active", LegacySQLTypeName.BOOLEAN)
        );

        List<FieldValueList> rows = Arrays.asList(
            createFieldValueList(schema, "John", 30L, 95.5, true),
            createFieldValueList(schema, "Jane", 25L, 88.2, false)
        );

        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);

        // Execute
        List<TestSimplePojo> result = BigQueryObjectReader.of(TestSimplePojo.class).read(tableResult);

        // Verify
        Truth.assertThat(result).hasSize(2);
        
        TestSimplePojo first = result.get(0);
        Truth.assertThat(first.name).isEqualTo("John");
        Truth.assertThat(first.age).isEqualTo(30);
        Truth.assertThat(first.score).isEqualTo(95.5);
        Truth.assertThat(first.active).isTrue();

        TestSimplePojo second = result.get(1);
        Truth.assertThat(second.name).isEqualTo("Jane");
        Truth.assertThat(second.age).isEqualTo(25);
        Truth.assertThat(second.score).isEqualTo(88.2);
        Truth.assertThat(second.active).isFalse();
    }

    @Test
    public void read_whenComplexTypes_thenMapsToPojoCorrectly(@Mock TableResult tableResult) {
        // Setup
        Schema schema = Schema.of(
            Field.of("id", LegacySQLTypeName.STRING),
            Field.of("birthDate", LegacySQLTypeName.DATE),
            Field.of("createdTime", LegacySQLTypeName.TIMESTAMP),
            Field.of("amount", LegacySQLTypeName.NUMERIC)
        );

        List<FieldValueList> rows = Arrays.asList(
            createFieldValueList(schema, "test-123", "2023-12-25", "2023-12-25T10:15:30Z", "123.45")
        );

        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);

        // Execute
        List<TestComplexPojo> result = BigQueryObjectReader.of(TestComplexPojo.class).read(tableResult);

        // Verify
        Truth.assertThat(result).hasSize(1);
        
        TestComplexPojo pojo = result.get(0);
        Truth.assertThat(pojo.id).isEqualTo("test-123");
        Truth.assertThat(pojo.birthDate).isEqualTo(LocalDate.of(2023, 12, 25));
        Truth.assertThat(pojo.createdTime).isEqualTo(Instant.parse("2023-12-25T10:15:30Z"));
        Truth.assertThat(pojo.amount).isEqualTo(new BigDecimal("123.45"));
    }

    @Test
    public void read_whenExplicitMappingUsed_thenMapsToCustomFieldNames(@Mock TableResult tableResult) {
        // Setup
        Schema schema = Schema.of(
            Field.of("user_id", LegacySQLTypeName.STRING),
            Field.of("user_name", LegacySQLTypeName.STRING),
            Field.of("user_age", LegacySQLTypeName.INTEGER)
        );

        List<FieldValueList> rows = Arrays.asList(
            createFieldValueList(schema, "id-123", "John Doe", 35L)
        );

        //when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);

        // Execute with explicit mapping
        List<TestDifferentFieldNamesPojo> result = BigQueryObjectReader.of(TestDifferentFieldNamesPojo.class)
            .map("user_id").to("identifier")
            .map("user_name").to("fullName")
            .map("user_age").to("years")
            .read(tableResult);

        // Verify
        Truth.assertThat(result).hasSize(1);
        
        TestDifferentFieldNamesPojo pojo = result.get(0);
        Truth.assertThat(pojo.identifier).isEqualTo("id-123");
        Truth.assertThat(pojo.fullName).isEqualTo("John Doe");
        Truth.assertThat(pojo.years).isEqualTo(35);
    }

    @Test
    public void read_whenCollections_thenMapsToListsCorrectly(@Mock TableResult tableResult) {
        // Setup
        Schema schema = Schema.of(
            Field.of("category", LegacySQLTypeName.STRING),
            Field.newBuilder("tags", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build(),
            Field.newBuilder("scores", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build()
        );

        List<FieldValue> tagValues = Arrays.asList(
            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "tag1"),
            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "tag2"),
            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "tag3")
        );

        List<FieldValue> scoreValues = Arrays.asList(
            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "10"),
            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "20"),
            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "30")
        );

        List<FieldValueList> rows = Arrays.asList(
            FieldValueList.of(
                Arrays.asList(
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test-category"),
                    FieldValue.of(FieldValue.Attribute.REPEATED, tagValues),
                    FieldValue.of(FieldValue.Attribute.REPEATED, scoreValues)
                ),
                schema.getFields()
            )
        );

        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);

        // Execute
        List<TestCollectionPojo> result = BigQueryObjectReader.of(TestCollectionPojo.class).read(tableResult);

        // Verify
        Truth.assertThat(result).hasSize(1);
        
        TestCollectionPojo pojo = result.get(0);
        Truth.assertThat(pojo.category).isEqualTo("test-category");
        Truth.assertThat(pojo.tags).containsExactly("tag1", "tag2", "tag3");
        Truth.assertThat(pojo.scores).containsExactly(10, 20, 30);
    }

    @Test
    public void read_whenNestedObjects_thenMapsRecursively(@Mock TableResult tableResult) {
        // Setup - nested object schema
        Schema nestedSchema = Schema.of(
            Field.of("name", LegacySQLTypeName.STRING),
            Field.of("age", LegacySQLTypeName.INTEGER)
        );

        Schema schema = Schema.of(
            Field.of("name", LegacySQLTypeName.STRING),
            Field.newBuilder("nested", LegacySQLTypeName.RECORD, nestedSchema.getFields()).build()
        );

        // Create nested field values
        FieldValueList nestedFieldValues = FieldValueList.of(
            Arrays.asList(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Nested Name"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "25")
            ),
            nestedSchema.getFields()
        );

        List<FieldValueList> rows = Arrays.asList(
            FieldValueList.of(
                Arrays.asList(
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Parent Name"),
                    FieldValue.of(FieldValue.Attribute.RECORD, nestedFieldValues)
                ),
                schema.getFields()
            )
        );

        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);

        // Execute
        List<TestNestedPojo> result = BigQueryObjectReader.of(TestNestedPojo.class).read(tableResult);

        // Verify
        Truth.assertThat(result).hasSize(1);
        
        TestNestedPojo pojo = result.get(0);
        Truth.assertThat(pojo.name).isEqualTo("Parent Name");
        Truth.assertThat(pojo.nested).isNotNull();
        Truth.assertThat(pojo.nested.name).isEqualTo("Nested Name");
        Truth.assertThat(pojo.nested.age).isEqualTo(25);
    }

    @Test
    public void read_whenNullValues_thenHandlesGracefully(@Mock TableResult tableResult) {
        // Setup
        Schema schema = Schema.of(
            Field.of("name", LegacySQLTypeName.STRING),
            Field.of("age", LegacySQLTypeName.INTEGER),
            Field.of("score", LegacySQLTypeName.FLOAT)
        );

        List<FieldValueList> rows = Arrays.asList(
            createFieldValueList(schema, null, null, null)
        );

        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);

        // Execute
        List<TestSimplePojo> result = BigQueryObjectReader.of(TestSimplePojo.class).read(tableResult);

        // Verify - null values should not cause exceptions and fields should remain default values
        Truth.assertThat(result).hasSize(1);
        
        TestSimplePojo pojo = result.get(0);
        Truth.assertThat(pojo.name).isNull();
        Truth.assertThat(pojo.age).isEqualTo(0); // default int value
        Truth.assertThat(pojo.score).isEqualTo(0.0); // default double value
    }

    @Test
    public void read_whenEmptyResult_thenReturnsEmptyList(@Mock TableResult tableResult) {
        // Setup
        Schema schema = Schema.of(
            Field.of("name", LegacySQLTypeName.STRING),
            Field.of("age", LegacySQLTypeName.INTEGER)
        );

        List<FieldValueList> rows = Collections.emptyList();
        
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);

        // Execute
        List<TestSimplePojo> result = BigQueryObjectReader.of(TestSimplePojo.class).read(tableResult);

        // Verify
        Truth.assertThat(result).isEmpty();
    }

    @Test
    public void read_whenNoExplicitMapping_thenInfersMappingsAutomatically(@Mock TableResult tableResult) {
        // Setup - schema matches POJO field names exactly
        Schema schema = Schema.of(
            Field.of("name", LegacySQLTypeName.STRING),
            Field.of("age", LegacySQLTypeName.INTEGER),
            Field.of("score", LegacySQLTypeName.FLOAT),
            Field.of("active", LegacySQLTypeName.BOOLEAN)
        );

        List<FieldValueList> rows = Arrays.asList(
            createFieldValueList(schema, "Test", 30L, 95.5, true)
        );

        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);

        // Execute without explicit mapping
        List<TestSimplePojo> result = BigQueryObjectReader.of(TestSimplePojo.class).read(tableResult);

        // Verify - automatic mapping should work
        Truth.assertThat(result).hasSize(1);
        
        TestSimplePojo pojo = result.get(0);
        Truth.assertThat(pojo.name).isEqualTo("Test");
        Truth.assertThat(pojo.age).isEqualTo(30);
        Truth.assertThat(pojo.score).isEqualTo(95.5);
        Truth.assertThat(pojo.active).isTrue();
    }

    @Test
    public void read_whenAllSupportedTypes_thenMapsEachTypeCorrectly(@Mock TableResult tableResult) {
        // Setup - test all supported types
        Schema schema = Schema.of(
            Field.of("stringField", LegacySQLTypeName.STRING),
            Field.of("byteField", LegacySQLTypeName.INTEGER),
            Field.of("shortField", LegacySQLTypeName.INTEGER),
            Field.of("intField", LegacySQLTypeName.INTEGER),
            Field.of("longField", LegacySQLTypeName.INTEGER),
            Field.of("doubleField", LegacySQLTypeName.FLOAT),
            Field.of("booleanField", LegacySQLTypeName.BOOLEAN),
            Field.of("bigDecimalField", LegacySQLTypeName.NUMERIC),
            //Field.of("bigIntegerField", LegacySQLTypeName.BIGNUMERIC),
            Field.of("localDateField", LegacySQLTypeName.DATE),
            Field.of("localTimeField", LegacySQLTypeName.TIME),
            Field.of("localDateTimeField", LegacySQLTypeName.DATETIME),
            Field.of("instantField", LegacySQLTypeName.TIMESTAMP)
        );

        List<FieldValueList> rows = Arrays.asList(
            createFieldValueList(schema, 
                "test string", "127", "32767", "2147483647", "9223372036854775807",
                "123.45", "true", "987.65", //"123456789012345678901234567890",
                "2023-10-27", "10:30:00", "2023-10-27T10:30:00", "2023-10-27T10:30:00Z"
            )
        );

        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.iterateAll()).thenReturn(rows);

        // Execute
        List<BigQueryObjectReader.AllTypesPojo> result = BigQueryObjectReader.of(BigQueryObjectReader.AllTypesPojo.class).read(tableResult);

        // Verify all types are correctly mapped
        Truth.assertThat(result).hasSize(1);
        
        BigQueryObjectReader.AllTypesPojo pojo = result.get(0);
        Truth.assertThat(pojo.stringField).isEqualTo("test string");
        Truth.assertThat(pojo.byteField).isEqualTo((byte) 127);
        Truth.assertThat(pojo.shortField).isEqualTo((short) 32767);
        Truth.assertThat(pojo.intField).isEqualTo(2147483647);
        Truth.assertThat(pojo.longField).isEqualTo(9223372036854775807L);
        Truth.assertThat(pojo.doubleField).isEqualTo(123.45);
        Truth.assertThat(pojo.booleanField).isTrue();
        Truth.assertThat(pojo.bigDecimalField).isEqualTo(new BigDecimal("987.65"));
        //Truth.assertThat(pojo.bigIntegerField).isEqualTo(new BigInteger("123456789012345678901234567890"));
        Truth.assertThat(pojo.localDateField).isEqualTo(LocalDate.of(2023, 10, 27));
        Truth.assertThat(pojo.localTimeField).isEqualTo(LocalTime.of(10, 30, 0));
        Truth.assertThat(pojo.localDateTimeField).isEqualTo(LocalDateTime.of(2023, 10, 27, 10, 30, 0));
        Truth.assertThat(pojo.instantField).isEqualTo(Instant.parse("2023-10-27T10:30:00Z"));
    }

    // Helper methods (same as before)
    private FieldValueList createFieldValueList(Schema schema, Object... values) {
        List<FieldValue> fieldValues = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            FieldValue fieldValue;
            
            if (value == null) {
                fieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, null);
            } else if (value instanceof String) {
                fieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, value.toString());
            } else if (value instanceof Long) {
                fieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, value.toString());
            } else if (value instanceof Double) {
                fieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, value.toString());
            } else if (value instanceof Boolean) {
                fieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, value.toString());
            } else {
                fieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, value.toString());
            }
            
            fieldValues.add(fieldValue);
        }
        return FieldValueList.of(fieldValues, schema.getFields());
    }
}