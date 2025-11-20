package com.safariyetu.commons.bigqueryobjects;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableMap;

/**
 * A utility class to simplify BigQuery operations, including automatic table creation,
 * schema updates, and data insertion via a fluent API.
 */
public class BigQueryObjectWriter {

    private final BigQuery bigquery;

    public BigQueryObjectWriter(BigQuery bigquery) {
        this.bigquery = bigquery;
    }

    /**
     * Fluent API entry point for building an insert request.
     * @param dataset The BigQuery dataset ID.
     * @param table The BigQuery table ID.
     * @return A builder for the insert operation.
     */
    public InsertBuilder insert(String dataset, String table) {
        return new InsertBuilder(dataset, table);
    }

    /**
     * Builder class for constructing and executing BigQuery insert requests.
     */
    public class InsertBuilder {
        private final TableId tableId;
        private final List<Object> objects = new ArrayList<>();
        private String timePartitioningField;
        private TimePartitioning.Type timePartitioningType = TimePartitioning.Type.DAY;
        private List<String> clusteringFields;

        public InsertBuilder(String dataset, String table) {
            this.tableId = TableId.of(dataset, table);
        }

        /**
         * Adds a single object to be inserted.
         * @param object The object to add.
         * @return This builder instance for chaining.
         */
        public InsertBuilder row(Object object) {
            this.objects.add(object);
            return this;
        }

        /**
         * Adds a collection of objects to be inserted.
         * @param objects The collection of objects to add.
         * @return This builder instance for chaining.
         */
        public InsertBuilder rows(Collection<? extends Object> objects) {
            this.objects.addAll(objects);
            return this;
        }

        /**
         * Specifies the field and type for time-based partitioning.
         * Defaults to DAY partitioning if not specified.
         * @param field The field to partition the table by.
         * @return This builder instance for chaining.
         */
        public InsertBuilder partitionBy(String field) {
            this.timePartitioningField = field;
            return this;
        }
        
        /**
         * Specifies the field and type for time-based partitioning.
         * @param field The field to partition the table by.
         * @param type The partitioning type (e.g., DAY, HOUR, MONTH, YEAR).
         * @return This builder instance for chaining.
         */
        public InsertBuilder partitionBy(String field, TimePartitioning.Type type) {
            this.timePartitioningField = field;
            this.timePartitioningType = type;
            return this;
        }

        /**
         * Specifies the fields for clustering the table.
         * @param fields The fields to cluster the table by.
         * @return This builder instance for chaining.
         */
        public InsertBuilder clusterBy(String... fields) {
            this.clusteringFields = Arrays.asList(fields);
            return this;
        }
        
        /**
         * Executes the insert operation, handling table creation and schema updates on failure.
         */
        public void execute() {
            try {
                // First, attempt to insert directly without checking the table.
                executeInsertInternal();
            } catch (BigQueryException e) {
                // Catch specific errors related to table or schema issues.
                if ("notFound".equals(e.getReason()) || e.getMessage().contains("schema mismatch")) {
                    System.err.println("BigQuery table not found or schema mismatch. Attempting to create/update table.");
                    try {
                        // Create or update the table based on the object's schema
                        createOrUpdateTable();
                        // Retry the insert after fixing the table.
                        executeInsertInternal();
                    } catch (BigQueryException createOrUpdateEx) {
                        System.err.println("Failed to create/update table or retry insert: " + createOrUpdateEx.getMessage());
                        throw new RuntimeException("Failed to handle BigQuery table error.", createOrUpdateEx);
                    }
                } else {
                    // Re-throw other unexpected BigQuery exceptions.
                    throw new RuntimeException("Failed to insert data into BigQuery with an unexpected error.", e);
                }
            }
        }
        
        private void executeInsertInternal() {
            if (objects.isEmpty()) {
                return;
            }

            InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);

            for (Object obj : objects) {
                Map<String, Object> rowContent = objectToMap(obj);
                builder.addRow(rowContent);
            }

            InsertAllResponse response = bigquery.insertAll(builder.build());

            if (response.hasErrors()) {
                response.getInsertErrors().forEach((row, errors) -> {
                    System.err.println("Error inserting row: " + errors);
                });
                throw new RuntimeException("Failed to insert all rows into BigQuery. See errors above.");
            }
        }

        private void createOrUpdateTable() {
            Table table = bigquery.getTable(tableId);
            Schema requiredSchema = getSchemaFromObjects(objects);

            // Start with a standard table definition builder
            StandardTableDefinition.Builder tableDefBuilder = StandardTableDefinition.newBuilder()
                .setSchema(requiredSchema);
            
            // Add time partitioning if specified
            if (timePartitioningField != null && !timePartitioningField.isEmpty()) {
                TimePartitioning timePartitioning = TimePartitioning.newBuilder(timePartitioningType)
                    .setField(timePartitioningField)
                    .build();
                tableDefBuilder.setTimePartitioning(timePartitioning);
            }

            // Add clustering if specified
            if (clusteringFields != null && !clusteringFields.isEmpty()) {
                Clustering clustering = Clustering.newBuilder().setFields(clusteringFields).build();
                tableDefBuilder.setClustering(clustering);
            }

            StandardTableDefinition tableDefinition = tableDefBuilder.build();
            
            if (table == null) {
                // Table does not exist, create it
                TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
                try {
                    bigquery.create(tableInfo);
                    System.out.println("Created table " + tableId.getDataset() + "." + tableId.getTable());
                } catch (BigQueryException e) {
                    throw new RuntimeException("Failed to create table in BigQuery", e);
                }
            } else {
                // Table exists, check if schema and other options need updating
                Schema currentSchema = table.getDefinition().getSchema();
                TableDefinition currentDefinition = table.getDefinition();
                
                boolean needsUpdate = !currentSchema.equals(requiredSchema)
                    || (timePartitioningField != null && !timePartitioningField.equals(
                        ((StandardTableDefinition) currentDefinition).getTimePartitioning().getField()))
                    || (clusteringFields != null && !clusteringFields.equals(
                        ((StandardTableDefinition) currentDefinition).getClustering().getFields()));
                
                if (needsUpdate) {
                    System.out.println("Schema or table options mismatch. Updating table " + tableId.getDataset() + "." + tableId.getTable());
                    Table updatedTable = table.toBuilder().setDefinition(tableDefinition).build();
                    try {
                        bigquery.update(updatedTable);
                        System.out.println("Updated table schema and options successfully.");
                    } catch (BigQueryException e) {
                        throw new RuntimeException("Failed to update table schema or options in BigQuery", e);
                    }
                }
            }
        }
    }

    /**
     * Dynamically generates the BigQuery schema from a list of objects.
     * It assumes all objects in the list are of the same type and have a flat structure or contain supported nested types.
     * The method uses reflection to inspect the class fields and map them to BigQuery data types.
     *
     * <h3>Supported Type Mappings:</h3>
     * <ul>
     * <li><code>int</code>, <code>long</code>, <code>Integer</code>, <code>Long</code>, <code>byte</code>, <code>Byte</code> -> <code>INTEGER</code></li>
     * <li><code>float</code>, <code>double</code>, <code>Float</code>, <code>Double</code> -> <code>FLOAT</code></li>
     * <li><code>boolean</code>, <code>Boolean</code> -> <code>BOOLEAN</code></li>
     * <li><code>String</code> -> <code>STRING</code></li>
     * <li><code>BigDecimal</code> -> <code>NUMERIC</code> (stored as a string)</li>
     * <li><code>LocalDate</code> -> <code>DATE</code> (stored as a string)</li>
     * <li><code>LocalDateTime</code> -> <code>DATETIME</code></li>
     * <li><code>Instant</code>, <code>Date</code>, <code>java.sql.Date</code> -> <code>TIMESTAMP</code></li>
     * <li><code>LocalTime</code> -> <code>TIME</code></li>
     * <li><code>Collection&lt;T&gt;</code> -> <code>REPEATED</code> field, where <code>T</code> is a supported type or a nested object.</li>
     * <li>Any other class -> <code>RECORD</code> field for nested objects.</li>
     * </ul>
     *
     * @param objects A list of objects to infer the schema from.
     * @return The generated BigQuery Schema.
     */
    protected Schema getSchemaFromObjects(List<Object> objects) {
        if (objects.isEmpty()) {
            throw new IllegalArgumentException("Cannot generate schema from an empty list of objects.");
        }
        return Schema.of(getFieldsFromClass(objects.get(0).getClass()));
    }

    /**
     * Converts an object to a map of key-value pairs, recursively handling nested objects
     * and collections. It handles the type conversions for BigQuery-compatible types.
     *
     * <h3>Type Conversion:</h3>
     * <ul>
     * <li><code>Temporal</code> and <code>Date</code> objects are converted to their <code>toString()</code> representation.</li>
     * <li><code>BigDecimal</code> is converted to its plain string representation via <code>toPlainString()</code>.</li>
     * <li><code>Collection</code> objects are mapped to a list of nested maps.</li>
     * <li>Primitive wrappers and <code>String</code>s are used directly.</li>
     * <li>Other objects are recursively converted to nested maps.</li>
     * </ul>
     *
     * @param obj The object to convert.
     * @return A map representing the object's fields and their values.
     */
    public Map<String, Object> objectToMap(Object obj) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        for (java.lang.reflect.Field reflectField : obj.getClass().getDeclaredFields()) {
            // Skip static and synthetic fields
            if (java.lang.reflect.Modifier.isStatic(reflectField.getModifiers()) || reflectField.isSynthetic()) {
                continue;
            }
            
            reflectField.setAccessible(true);
            try {
                Object value = reflectField.get(obj);
                if (value != null) {
                    // Handle collections first
                    if (Collection.class.isAssignableFrom(reflectField.getType())) {
                        java.lang.reflect.Type genericType = reflectField.getGenericType();
                        if (genericType instanceof java.lang.reflect.ParameterizedType) {
                            java.lang.reflect.Type elementType = ((java.lang.reflect.ParameterizedType) genericType).getActualTypeArguments()[0];
                            if (elementType instanceof Class) {
                                Class<?> elementClass = (Class<?>) elementType;
                                
                                Collection<?> collection = (Collection<?>) value;
                                List<?> mappedCollection;
                                // Check if the collection contains primitive types or strings
                                if (isPrimitiveWrapperOrString(elementClass)) {
                                   mappedCollection = new ArrayList<>(collection);
                                } else {
                                   // Assume it's a collection of complex objects and map them
                                   mappedCollection = collection.stream()
                                       .map(this::objectToMap)
                                       .collect(Collectors.toList());
                                }
                                builder.put(reflectField.getName(), mappedCollection);
                            }
                        }
                    } else {
                        // Handle non-collection types
                        LegacySQLTypeName type = getTypeFromClass(reflectField.getType());
                        if (type != null) {
                            // Primitive and supported simple types
                            builder.put(reflectField.getName(), getValueForSupportedSimpleType(reflectField.getType(), value));
                        } else {
                            // Handle nested objects recursively
                            builder.put(reflectField.getName(), objectToMap(value));
                        }
                    }
                }
            } catch (IllegalAccessException e) {
                System.err.println("Error accessing field: " + reflectField.getName());
            }
        }
        return builder.build();
    }

    /**
     * Dynamically gets the BigQuery fields from a Java class using reflection.
     * Ignores static fields as they are class-level, not instance-level data.
     * @param clazz The class to inspect.
     * @return A list of BigQuery fields.
     */
    public List<com.google.cloud.bigquery.Field> getFieldsFromClass(Class<?> clazz) {
        List<com.google.cloud.bigquery.Field> bqFields = new ArrayList<>();
        for (java.lang.reflect.Field reflectField : clazz.getDeclaredFields()) {
            // Skip static and synthetic fields
            if (java.lang.reflect.Modifier.isStatic(reflectField.getModifiers()) || reflectField.isSynthetic()) {
                continue;
            }
            
            // Handle collections first
            if (Collection.class.isAssignableFrom(reflectField.getType())) {
                java.lang.reflect.Type genericType = reflectField.getGenericType();
                if (genericType instanceof java.lang.reflect.ParameterizedType) {
                    java.lang.reflect.Type elementType = ((java.lang.reflect.ParameterizedType) genericType).getActualTypeArguments()[0];
                    if (elementType instanceof Class) {
                        Class<?> elementClass = (Class<?>) elementType;
                        LegacySQLTypeName repeatedType = getTypeFromClass(elementClass);
                        if (repeatedType != null) {
                            // Repeated primitive and supported simple types
                            bqFields.add(
                                    com.google.cloud.bigquery.Field.newBuilder(reflectField.getName(), repeatedType)
                                    .setMode(com.google.cloud.bigquery.Field.Mode.REPEATED).build());
                        } else {
                            // Repeated nested objects
                            final FieldList objectFields = FieldList.of(getFieldsFromClass(elementClass));
                            bqFields.add(
                                    com.google.cloud.bigquery.Field.newBuilder(reflectField.getName(), LegacySQLTypeName.RECORD, objectFields)
                                    .setMode(com.google.cloud.bigquery.Field.Mode.REPEATED).build());
                        }
                    }
                }
            } else {
                // Handle non-collection types
                LegacySQLTypeName type = getTypeFromClass(reflectField.getType());
                if (type != null) {
                    // Primitive and supported simple types
                    bqFields.add(com.google.cloud.bigquery.Field.of(reflectField.getName(), type));
                } else {
                    // This is a nested record
                    final FieldList nestedObjectFields = FieldList.of(getFieldsFromClass(reflectField.getType()));
                    bqFields.add(com.google.cloud.bigquery.Field.newBuilder(reflectField.getName(), LegacySQLTypeName.RECORD, nestedObjectFields).build());
                }
            }
        }
        return bqFields;
    }

    public LegacySQLTypeName getTypeFromClass(Class<?> clazz) {
        if (clazz == Integer.class || clazz == int.class || clazz == Long.class || clazz == long.class || clazz == Byte.class || clazz == byte.class) {
            return LegacySQLTypeName.INTEGER;
        } else if (clazz == String.class) {
            return LegacySQLTypeName.STRING;
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return LegacySQLTypeName.BOOLEAN;
        } else if (clazz == Double.class || clazz == double.class || clazz == Float.class || clazz == float.class) {
            return LegacySQLTypeName.FLOAT;
        } else if (clazz == BigDecimal.class) {
            return LegacySQLTypeName.NUMERIC;
        } else if (clazz == LocalDate.class) {
            return LegacySQLTypeName.DATE;
        } else if (clazz == LocalDateTime.class) {
            return LegacySQLTypeName.DATETIME;
        } else if (clazz == Instant.class || clazz == Date.class || clazz == java.sql.Date.class) {
            return LegacySQLTypeName.TIMESTAMP;
        } else if (clazz == LocalTime.class) {
            return LegacySQLTypeName.TIME;
        }
        return null; // Return null for nested objects or unsupported types.
    }
    
    public boolean isPrimitiveWrapperOrString(Object obj) {
        return obj instanceof Number || obj instanceof Boolean || obj instanceof String;
    }
    
    public boolean isPrimitiveWrapperOrString(Class<?> clazz) {
        return Number.class.isAssignableFrom(clazz) || clazz == Boolean.class || clazz == String.class || clazz.isPrimitive();
    }
    
    private Object getValueForSupportedSimpleType(Class<?> clazz, Object value) {
        if (value instanceof BigDecimal) {
            // NUMERIC
            return ((BigDecimal)value).toPlainString();
        } else if (value instanceof Date) {
        	return Instant.ofEpochMilli(((Date)value).getTime()).toString();
        } else if (value instanceof Temporal) {
            // Handle java.time.temporal.Temporal types by converting to ISO 8601 string
            // DATE, DATETIME, TIMESTAMP, TIME
            return value.toString();
        } else if (isPrimitiveWrapperOrString(value)) {
            //(clazz == Integer.class || clazz == int.class || clazz == Long.class || clazz == long.class || clazz == Byte.class || clazz == byte.class)
            return value;
        }
        // Should not happen. 
        // If we reach here the type was recognized by getTypeFromClass() 
        throw new IllegalArgumentException("Unsupported type: "+clazz+" ("+value+")");
    }

}
