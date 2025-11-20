package com.safariyetu.commons.bigqueryobjects;

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;

/**
 * A fluent API for mapping BigQuery query results (TableResult) to a list of POJOs.
 * This class uses reflection to set field values based on column-to-field mappings.
 * It automatically maps fields by name by default, but allows for explicit overrides
 * using the fluent API.
 *
 * @param <T> The type of the POJO to which the results will be mapped.
 */
public class BigQueryObjectReader<T> {

    // A map to store the user-defined mapping from BigQuery column names to POJO field names.
    private final Map<String, String> explicitColumnToFieldMap = new HashMap<>();

    // The class of the POJO we are mapping to.
    private final Class<T> pojoClass;

    // A temporary holder for the current column being mapped in the fluent chain.
    private String currentColumn;

    // A private constructor to enforce the use of the static factory method.
    private BigQueryObjectReader(Class<T> pojoClass) {
        this.pojoClass = pojoClass;
    }

    /**
     * The starting point of the fluent API. Creates a new instance of the reader.
     *
     * @param pojoClass The Class object of the POJO to which the results will be mapped.
     * @param <T>       The type of the POJO.
     * @return A new instance of BigQueryObjectReader.
     */
    public static <T> BigQueryObjectReader<T> of(Class<T> pojoClass) {
        return new BigQueryObjectReader<>(pojoClass);
    }

    /**
     * Specifies the BigQuery column name to map.
     *
     * @param columnName The name of the BigQuery column.
     * @return The current instance of the reader for method chaining.
     */
    public BigQueryObjectReader<T> map(String columnName) {
        this.currentColumn = columnName;
        return this;
    }

    /**
     * Specifies the POJO field name that the current column maps to.
     * This method completes the mapping for the current column.
     *
     * @param fieldName The name of the POJO field.
     * @return The current instance of the reader for method chaining.
     */
    public BigQueryObjectReader<T> to(String fieldName) {
        if (this.currentColumn == null) {
            throw new IllegalStateException("Must call 'map' before calling 'to'.");
        }
        this.explicitColumnToFieldMap.put(this.currentColumn, fieldName);
        this.currentColumn = null; // Clear the temporary holder for the next mapping.
        return this;
    }


    /**
     * Reads the results from a BigQuery TableResult and maps them to a list of POJOs.
     *
     * @param result The TableResult containing the query results.
     * @return A List of POJO instances populated with data from the query results.
     */
    public List<T> read(TableResult result) {
        // If no explicit mappings are provided, try to infer them by name.
        if (explicitColumnToFieldMap.isEmpty()) {
            inferMappings(result.getSchema());
        }

        List<T> pojos = new ArrayList<>();
        try {
            // Get the constructor of the POJO.
            Constructor<T> constructor = pojoClass.getDeclaredConstructor();
            constructor.setAccessible(true);

            // Get the fields of the POJO using reflection.
            java.lang.reflect.Field[] fields = pojoClass.getDeclaredFields();
            Map<String, java.lang.reflect.Field> fieldMap = new HashMap<>();
            for (java.lang.reflect.Field field : fields) {
                fieldMap.put(field.getName(), field);
            }

            // Iterate through each row in the TableResult.
            for (FieldValueList row : result.iterateAll()) {
                // Create a new instance of the POJO.
                T pojo = constructor.newInstance();

                // Iterate through the registered column-to-field mappings.
                for (Map.Entry<String, String> entry : explicitColumnToFieldMap.entrySet()) {
                    String columnName = entry.getKey();
                    String pojoFieldName = entry.getValue();

                    // Get the FieldValue for the current column.
                    FieldValue value = row.get(columnName);

                    // Find the corresponding POJO field.
                    java.lang.reflect.Field field = fieldMap.get(pojoFieldName);

                    if (field != null) {
                        field.setAccessible(true);
                        // Set the value of the field based on its type.
                        if (value != null && !value.isNull()) {
                            Object typedValue = getTypedValue(value, field);
                            field.set(pojo, typedValue);
                        }
                    }
                }
                pojos.add(pojo);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error mapping BigQuery result to POJO", e);
        }
        return pojos;
    }

    /**
     * Infers the column-to-field mappings based on matching names in the schema.
     *
     * @param schema The BigQuery schema from the query result.
     */
    private void inferMappings(Schema schema) {
        for (Field schemaField : schema.getFields()) {
            String columnName = schemaField.getName();
            try {
                // Check if a field with the same name exists in the POJO.
                pojoClass.getDeclaredField(columnName);
                explicitColumnToFieldMap.put(columnName, columnName);
            } catch (NoSuchFieldException e) {
                // Ignore if no matching field is found.
            }
        }
    }

    /**
     * Helper method to convert a FieldValue to the correct type for the POJO field.
     *
     * @param value The FieldValue from BigQuery.
     * @param field The POJO field to which the value will be set.
     * @return The converted value as an Object.
     */
    private Object getTypedValue(FieldValue value, java.lang.reflect.Field field) {
        Class<?> type = field.getType();
        // Handle repeated fields (collections)
        if (value.getAttribute() == FieldValue.Attribute.REPEATED) {
            List<Object> collection = new ArrayList<>();
            // Get the generic type of the list
            Type genericType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
            for (FieldValue repeatedValue : value.getRepeatedValue()) {
                // Recursively call to get the typed value for each element in the collection.
                // This now properly handles collections of both simple and complex types.
                collection.add(getTypedValue(repeatedValue, genericType));
            }
            return collection;
        }

        // Handle nested records
        if (value.getAttribute() == FieldValue.Attribute.RECORD) {
            return readNestedObject(value.getRecordValue(), type);
        }
        
        // Handle simple types
        return getTypedValue(value, type);
    }

    /**
     * A helper method to convert a FieldValue to a specific type, including handling complex types.
     * This method is called recursively to support nested objects and collections.
     *
     * @param value The FieldValue from BigQuery.
     * @param type The Type object of the target field.
     * @return The converted value as an Object.
     */
    private Object getTypedValue(FieldValue value, Type type) {
        if (type instanceof Class) {
            return getTypedValue(value, (Class<?>) type);
        } else if (type instanceof ParameterizedType) {
            Type rawType = ((ParameterizedType) type).getRawType();
            if (rawType instanceof Class && List.class.isAssignableFrom((Class<?>) rawType)) {
                // This is a list. Recursively call to get the typed value for each element.
                List<Object> collection = new ArrayList<>();
                Type genericType = ((ParameterizedType) type).getActualTypeArguments()[0];
                for (FieldValue repeatedValue : value.getRepeatedValue()) {
                    collection.add(getTypedValue(repeatedValue, genericType));
                }
                return collection;
            }
        }
        return value.getValue(); // Fallback
    }

    /**
     * Helper method to convert a FieldValue to the correct type for the POJO field.
     *
     * @param value The FieldValue from BigQuery.
     * @param type  The Class of the POJO field.
     * @return The converted value as an Object.
     */
    private Object getTypedValue(FieldValue value, Class<?> type) {
        if (type == String.class) {
            return value.getStringValue();
        } else if (type == Byte.class || type == byte.class) {
            return (byte) value.getLongValue();
        } else if (type == Short.class || type == short.class) {
            return (short) value.getLongValue();
        } else if (type == Integer.class || type == int.class) {
            return (int) value.getLongValue();
        } else if (type == Long.class || type == long.class) {
            return value.getLongValue();
        } else if (type == Double.class || type == double.class) {
            return value.getDoubleValue();
        } else if (type == Boolean.class || type == boolean.class) {
            return value.getBooleanValue();
        } else if (type == BigDecimal.class) {
            return new BigDecimal(value.getStringValue());
        } else if (type == BigInteger.class) {
            return new BigInteger(value.getStringValue());
        } else if (type == LocalDate.class) {
            return LocalDate.parse(value.getStringValue());
        } else if (type == LocalTime.class) {
            return LocalTime.parse(value.getStringValue());
        } else if (type == LocalDateTime.class) {
        	return parseLocalDateTime(value.getStringValue());
        } else if (type == OffsetDateTime.class) {
        	return parseOffsetDateTime(value);
        } else if (type == Instant.class) {
        	return parseInstant(value);
        } else if (type == Date.class) {
        	return parseDate(value);
        } else if (isPrimitiveOrWrapper(type)) {
            // Handle primitive and wrapper types
            return getTypedValue(value, (Class<?>) type);
        } else {
            // Handle nested objects
            return readNestedObject(value.getRecordValue(), type);
        }
    }

    /**
     * Checks if a given class is a primitive or a wrapper class.
     *
     * @param type The Class to check.
     * @return true if the class is a primitive or a wrapper, false otherwise.
     */
    private boolean isPrimitiveOrWrapper(Class<?> type) {
        return type.isPrimitive() ||
               type == Boolean.class || type == Byte.class || type == Character.class ||
               type == Short.class || type == Integer.class || type == Long.class ||
               type == Float.class || type == Double.class;
    }

    /**
     * Recursively maps a BigQuery record to a nested POJO.
     *
     * @param record The FieldValueList representing the nested record.
     * @param pojoClass The class of the nested POJO.
     * @param <T> The type of the POJO.
     * @return A new instance of the nested POJO populated with data.
     */
    private <R> R readNestedObject(FieldValueList record, Class<R> pojoClass) {
        try {
            R pojo = pojoClass.getDeclaredConstructor().newInstance();
            java.lang.reflect.Field[] fields = pojoClass.getDeclaredFields();

            for (java.lang.reflect.Field field : fields) {
                field.setAccessible(true);
                FieldValue value = null;
                try {
                    // Get the FieldValue by name, handling cases where the column might be missing.
                    value = record.get(field.getName());
                } catch (IllegalArgumentException e) {
                    // The column for this field does not exist in the record.
                    // We can simply skip this field. Optionally, log a warning.
                    System.err.println("Warning: Column '" + field.getName() + "' not found in nested BigQuery record. Skipping field.");
                    continue;
                }
                
                if (value != null && !value.isNull()) {
                    field.set(pojo, getTypedValue(value, field));
                }
            }
            return pojo;
        } catch (Exception e) {
            throw new RuntimeException("Error mapping nested BigQuery record to POJO", e);
        }
    }

    /**
     * Parse LocalDateTime from string, handling UTC format
     */
    private LocalDateTime parseLocalDateTime(String stringValue) {
        if (stringValue.endsWith(" UTC")) {
            return parseBigQueryUtcString(stringValue).atOffset(ZoneOffset.UTC).toLocalDateTime();
        }
        try {
            return LocalDateTime.parse(stringValue);
        } catch (DateTimeParseException e) {
            // Try ISO format without timezone
            return LocalDateTime.parse(stringValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }

    }

    /**
     * Parse OffsetDateTime from string, handling UTC format
     */
    private OffsetDateTime parseOffsetDateTime(FieldValue value) {
    	String stringValue = value.getStringValue();
    	if (NumberUtils.isNumber(stringValue)) {
    		// Assume TIMESTAMP field 
    		return Instant.ofEpochMilli(value.getTimestampValue() / 1000)
    				.atOffset(ZoneOffset.UTC);
    	}

    	if (stringValue.endsWith(" UTC")) {
            return parseBigQueryUtcString(stringValue).atOffset(ZoneOffset.UTC);
        }
        try {
            return OffsetDateTime.parse(stringValue);
        } catch (DateTimeParseException e) {
            // Try ISO format without timezone
            return OffsetDateTime.parse(stringValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

    }
    
    private LocalDateTime parseBigQueryUtcString(String stringValue) {
    	return LocalDateTime.parse(
                stringValue.substring(0, stringValue.length() - 4),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            );
    }
    
    /**
     * Parse Instant from string, handling UTC format
     */
    private Instant parseInstant(FieldValue value) {
    	final String stringValue = value.getStringValue();
		if (NumberUtils.isNumber(stringValue)) {
    		// Assume TIMESTAMP field 
    		return Instant.ofEpochMilli(value.getTimestampValue() / 1000);
    	}
    	if (stringValue.endsWith(" UTC")) {
            return parseBigQueryUtcString(stringValue).atOffset(ZoneOffset.UTC).toInstant();
        }
        try {
            return Instant.parse(stringValue);
        } catch (DateTimeParseException e) {
            // Try parsing as LocalDateTime first
            try {
                return LocalDateTime.parse(stringValue).toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException ex) {
                throw new DateTimeParseException("Failed to parse Instant: " + stringValue, stringValue, 0);
            }
        }
    }

    /**
     * Parse Date from string, handling UTC format
     */
    private Date parseDate(FieldValue value) {
    	final String stringValue = value.getStringValue();
		if (NumberUtils.isNumber(stringValue)) {
    		// Assume TIMESTAMP field 
    		return Date.from(Instant.ofEpochMilli(value.getTimestampValue() / 1000));
    	}
		if (stringValue.endsWith(" UTC")) {
            return Date.from(parseBigQueryUtcString(stringValue).atOffset(ZoneOffset.UTC).toInstant());
        }
        return Date.from(parseInstant(value));
    }

    /**
     * A POJO that demonstrates mapping to all primitive and new date/time types.
     */
    public static class AllTypesPojo {
        public String stringField;
        public byte byteField;
        public short shortField;
        public int intField;
        public long longField;
        public double doubleField;
        public boolean booleanField;
        public BigDecimal bigDecimalField;
        public BigInteger bigIntegerField;
        public LocalDate localDateField;
        public LocalTime localTimeField;
        public LocalDateTime localDateTimeField;
        public OffsetDateTime offsetDateTimeField;
        public Instant instantField;
        public Date dateField;

        public String toString() {
            return "AllTypesPojo{" +
                   "stringField='" + stringField + '\'' +
                   ", byteField=" + byteField +
                   ", shortField=" + shortField +
                   ", intField=" + intField +
                   ", longField=" + longField +
                   ", doubleField=" + doubleField +
                   ", booleanField=" + booleanField +
                   ", bigDecimalField=" + bigDecimalField +
                   ", bigIntegerField=" + bigIntegerField +
                   ", localDateField=" + localDateField +
                   ", localTimeField=" + localTimeField +
                   ", localDateTimeField=" + localDateTimeField +
                   ", offsetDateTimeField=" + offsetDateTimeField +
                   ", instantField=" + instantField +
                   ", dateField=" + dateField +
                   '}';
        }
    }

    /**
     * POJO with different field names for explicit mapping
     */
    public static class InventoryItem {
        private String id;
        private String name;
        private double cost;
        private int quantity;

        public InventoryItem() {}
        public String toString() {
            return "InventoryItem{id='" + id + "', name='" + name + "', cost=" + cost + ", quantity=" + quantity + '}';
        }
    }

    /**
     * New POJO for nested object example
     */
    public static class Address {
        private String street;
        private String city;
        private String postalCode;

        public Address() {}
        public String toString() {
            return "Address{street='" + street + "', city='" + city + "', postalCode='" + postalCode + "'}";
        }
    }

    /**
     * New POJO with nested object and list of addresses
     */
    public static class User {
        private String id;
        private String name;
        private List<String> phoneNumbers;
        private List<Address> addresses;

        public User() {}
        public String toString() {
            return "User{id='" + id + "', name='" + name + "', phoneNumbers=" + phoneNumbers + ", addresses=" + addresses + '}';
        }
    }

    /**
     * A simple main method to demonstrate the usage.
     * NOTE: This does not connect to a real BigQuery instance. It uses mock data.
     */
    public static void main(String[] args) {
        // --- Mocking a TableResult for the example ---
        // In a real application, you would get this from a BigQuery query.

        // -----------------------------------------------------------
        // DEMO 1: Automatic Mapping for a POJO with All Supported Types
        // -----------------------------------------------------------
        System.out.println("-----------------------------------------------------------");
        System.out.println("DEMO 1: Automatic Mapping (all supported types)");
        System.out.println("-----------------------------------------------------------");
        Schema allTypesSchema = Schema.of(
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
            Field.of("offsetDateTimeField", LegacySQLTypeName.TIMESTAMP),
            Field.of("instantField", LegacySQLTypeName.TIMESTAMP),
            Field.of("dateField", LegacySQLTypeName.TIMESTAMP)
        );

        List<FieldValueList> allTypesRows = new ArrayList<>();
        allTypesRows.add(FieldValueList.of(
            ImmutableList.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test string"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "127"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "32767"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2147483647"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "9223372036854775807"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123.45"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "987.65"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123456789012345678901234567890"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2023-10-27"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "10:30:00"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2023-10-27T10:30:00"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2023-10-27 10:30:00 UTC"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2023-10-27 10:30:00 UTC"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2023-10-27 10:30:00 UTC")
            ), allTypesSchema.getFields()
        ));

        TableResult allTypesResult = //new TableResult(allTypesSchema, allTypesRows.size(), new MockPage(allTypesRows));
		        TableResult.newBuilder()
		    	.setSchema(allTypesSchema)
		    	.setTotalRows(Long.valueOf(allTypesRows.size()))
		    	.setPageNoSchema(new MockPage(allTypesRows)).build();
        List<AllTypesPojo> allTypesPojos = BigQueryObjectReader.of(AllTypesPojo.class).read(allTypesResult);
        allTypesPojos.forEach(System.out::println);

        // -----------------------------------------------------------
        // DEMO 2: Explicit Mapping for different POJO field names
        // -----------------------------------------------------------
        System.out.println("\n-----------------------------------------------------------");
        System.out.println("DEMO 2: Explicit Mapping (mismatched names)");
        System.out.println("-----------------------------------------------------------");
        Schema productSchema = Schema.of(
            Field.of("item_id", LegacySQLTypeName.STRING),
            Field.of("product_name", LegacySQLTypeName.STRING),
            Field.of("unit_cost", LegacySQLTypeName.FLOAT),
            Field.of("total_quantity", LegacySQLTypeName.INTEGER)
        );
        List<FieldValueList> productRows = new ArrayList<>();
        productRows.add(FieldValueList.of(
            ImmutableList.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "SKU-123"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Example Product"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "9.99"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "100")
            ), productSchema.getFields()
        ));
        TableResult productResult = //new TableResult(productSchema, productRows.size(), new MockPage(productRows));
		        TableResult.newBuilder()
		        	.setSchema(productSchema)
		        	.setTotalRows(Long.valueOf(productRows.size()))
		        	.setPageNoSchema(new MockPage(productRows)).build();
        List<InventoryItem> items = BigQueryObjectReader.of(InventoryItem.class)
            .map("item_id").to("id")
            .map("product_name").to("name")
            .map("unit_cost").to("cost")
            .map("total_quantity").to("quantity")
            .read(productResult);
        items.forEach(System.out::println);

        // -----------------------------------------------------------
        // DEMO 3: Mapping Complex, Nested Objects and Repeated Fields
        // -----------------------------------------------------------
        System.out.println("\n-----------------------------------------------------------");
        System.out.println("DEMO 3: Nested Objects and Repeated Fields");
        System.out.println("-----------------------------------------------------------");
        Schema userSchema = Schema.of(
            Field.of("id", LegacySQLTypeName.STRING),
            Field.of("name", LegacySQLTypeName.STRING),
            Field.newBuilder("phoneNumbers", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build(),
            Field.newBuilder(
                "addresses",
                LegacySQLTypeName.RECORD,
                Field.of("street", LegacySQLTypeName.STRING),
                Field.of("city", LegacySQLTypeName.STRING),
                Field.of("postalCode", LegacySQLTypeName.STRING)
            ).setMode(Field.Mode.REPEATED).build()
        );
        List<FieldValueList> userRows = new ArrayList<>();
        userRows.add(FieldValueList.of(
            ImmutableList.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "USR-1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Jane Doe"),
                FieldValue.of(FieldValue.Attribute.REPEATED, ImmutableList.of(
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "555-1234"),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "555-5678")
                )),
                FieldValue.of(FieldValue.Attribute.REPEATED, ImmutableList.of(
                    FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(
                        ImmutableList.of(
                            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123 Main St"),
                            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Anytown"),
                            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "12345")
                        ), userSchema.getFields().get(3).getSubFields()
                    )),
                    FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(
                         ImmutableList.of(
                             FieldValue.of(FieldValue.Attribute.PRIMITIVE, "456 Oak Ave"),
                             FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Another City"),
                             FieldValue.of(FieldValue.Attribute.PRIMITIVE, "67890")
                         ), userSchema.getFields().get(3).getSubFields()
                    ))
                ))
            ), userSchema.getFields()
        ));
        TableResult userResult = //new TableResult(userSchema, userRows.size(), new MockPage(userRows));
		        TableResult.newBuilder()
		    	.setSchema(userSchema)
		    	.setTotalRows(Long.valueOf(userRows.size()))
		    	.setPageNoSchema(new MockPage(userRows))
		    	.build();
        List<User> users = BigQueryObjectReader.of(User.class).read(userResult);
        users.forEach(System.out::println);
    }

    /**
     * A mock implementation of the Page interface for testing.
     */
    static class MockPage implements Page<FieldValueList> {
        private final List<FieldValueList> rows;
        public MockPage(List<FieldValueList> rows) {
            this.rows = rows;
        }
        @Override public boolean hasNextPage() { return false; }
        @Override public String getNextPageToken() { return null; }
        @Override public Page<FieldValueList> getNextPage() { return null; }
        @Override public Iterable<FieldValueList> iterateAll() { return rows; }
        @Override public Iterable<FieldValueList> getValues() { return rows; }
    }
}
