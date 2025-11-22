# Flink Connector Template

This template provides a starting point for creating custom Apache Flink table connectors. It includes:

- Template implementation classes for table sources using Flink 2.x Source API
- Unit tests
- Integration tests
- Maven build configuration
- Service loader configuration

**Built for Apache Flink 2.1.1** | Requires Java 11+ (Java 17 recommended)

## Getting Started

### 1. Copy the Template

Copy this entire `flink-connector-template` directory to your new project location:

```bash
cp -r flink-connector-template /path/to/your-new-connector
cd /path/to/your-new-connector
```

### 2. Customize Package and Connector Name

Replace all placeholders throughout the project:

**Placeholders to replace:**
- `{{CONNECTOR_NAME}}` - Your connector name (e.g., "kafka", "http", "redis")
- `com.example.connector` - Your package name (e.g., "com.mycompany.flink.http")

**Files to update:**
1. `pom.xml`
   - `<groupId>` and `<artifactId>`
   - `{{CONNECTOR_NAME}}` placeholder
   - Add your connector-specific dependencies

2. Rename package directory:
   ```bash
   # In src/main/java and src/test/java
   mv com/example/connector com/mycompany/flink/yourconnector
   ```

3. Update all Java files:
   - Replace package declarations
   - Replace `{{CONNECTOR_NAME}}` with your connector identifier
   - Update class names (e.g., `ConnectorTableSourceFactory` → `YourConnectorTableSourceFactory`)

4. Update service loader file:
   - `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`
   - Replace with your factory's fully qualified class name

### 3. Implement Your Connector

#### A. Define Configuration Options (ConnectorTableSourceFactory.java)

1. Update `IDENTIFIER` constant with your connector name
2. Define connector-specific `ConfigOption` fields:
   ```java
   public static final ConfigOption<String> YOUR_OPTION =
       key("your-option")
           .stringType()
           .defaultValue("default-value")
           .withDescription("Description of your option");
   ```
3. Update `requiredOptions()` and `optionalOptions()` methods
4. Implement validation logic in `validateConfiguration()`
5. Update `createDynamicTableSource()` to pass correct parameters

#### B. Implement Data Source (Flink 2.x Source API)

The template uses the new Source API (mandatory in Flink 2.x). You'll need to implement:

1. **ConnectorSource** - Main source interface:
   - Define boundedness (bounded or unbounded)
   - Create SourceReader and SplitEnumerator instances

2. **ConnectorSourceReader** - Reads data from splits:
   - Initialize connector client/connection
   - Read data in `pollNext()` method
   - Convert source data to `RowData` format
   - Handle checkpointing via `snapshotState()`

3. **ConnectorSplitEnumerator** - Manages split discovery and assignment:
   - Discover splits from your source (files, partitions, etc.)
   - Assign splits to readers
   - Handle dynamic split discovery if needed

4. **ConnectorSourceSplit** - Represents a portion of your data:
   - Define what constitutes a split (file, partition, offset range, etc.)
   - Make it serializable for checkpointing

#### C. Implement Type Conversion (TypeConversionUtils.java)

1. Customize `stringValueToType()` for your data format
2. Add additional conversion methods if needed (JSON, Avro, Protobuf, etc.)
3. Handle all data types your connector supports

#### D. (Optional) Implement Lookup Function

If your connector supports lookup joins (dimension table lookups):

1. Edit `ConnectorLookupFunction.java`
2. Implement lookup logic in `lookup()` method
3. Update `ConnectorTableSource.java` to implement `LookupTableSource`

If not needed, delete `ConnectorLookupFunction.java` and remove `LookupTableSource` interface from `ConnectorTableSource`.

### 4. Customize Tests

#### Unit Tests (ConnectorTableSourceFactoryTest.java)
1. Update test schemas to match your supported data types
2. Add tests for your connector-specific configuration options
3. Test validation logic for invalid configurations

#### Unit Tests (ConnectorSourceReaderTest.java)
1. Test split assignment and reading logic
2. Test type conversion for all supported types
3. Test error handling and checkpointing
4. Test split enumerator discovery logic

#### Integration Tests (ConnectorIntegrationTest.java)
1. Update DDL statements with your connector's configuration
2. Add tests for all supported data types
3. Test various connector configurations
4. Consider adding tests with actual external resources

### 5. Update Dependencies

In `pom.xml`, add your connector-specific dependencies:

```xml
<dependency>
  <groupId>your.dependency</groupId>
  <artifactId>client-library</artifactId>
  <version>x.y.z</version>
</dependency>
```

### 6. Build and Test

```bash
# Format code
mvn spotless:apply

# Build the project
mvn clean package

# Run tests
mvn test

# Run integration tests
mvn verify
```

### 7. Usage Example

After building your connector, use it in Flink SQL:

```sql
CREATE TABLE your_table (
    id BIGINT,
    name STRING,
    value DOUBLE
) WITH (
    'connector' = 'your-connector-name',
    'endpoint' = 'your-endpoint-url',
    'batch-size' = '100'
    -- Add your connector-specific options
);

SELECT * FROM your_table;
```

## Project Structure

```
flink-connector-template/
├── pom.xml                                 # Maven build configuration
├── src/
│   ├── main/
│   │   ├── java/com/example/connector/
│   │   │   ├── ConnectorTableSourceFactory.java    # Factory for creating table sources
│   │   │   ├── ConnectorTableSource.java           # Table source implementation
│   │   │   ├── ConnectorSource.java                # Source API implementation (Flink 2.x)
│   │   │   ├── ConnectorSourceReader.java          # Reads data from splits
│   │   │   ├── ConnectorSplitEnumerator.java       # Manages split discovery
│   │   │   ├── ConnectorSourceSplit.java           # Represents a data split
│   │   │   ├── ConnectorEnumeratorState.java       # Enumerator checkpoint state
│   │   │   ├── ConnectorSourceSplitSerializer.java # Split serializer
│   │   │   ├── ConnectorEnumeratorStateSerializer.java # State serializer
│   │   │   ├── ConnectorLookupFunction.java        # Lookup join support (optional)
│   │   │   └── TypeConversionUtils.java            # Type conversion utilities
│   │   └── resources/
│   │       └── META-INF/services/
│   │           └── org.apache.flink.table.factories.Factory  # Service loader config
│   └── test/
│       ├── java/com/example/connector/
│       │   ├── ConnectorTableSourceFactoryTest.java    # Unit tests for factory
│       │   ├── ConnectorSourceFunctionTest.java        # Unit tests for source reader
│       │   └── ConnectorIntegrationTest.java           # Integration tests
│       └── resources/
│           └── log4j2.properties                       # Test logging configuration
└── README.md                               # This file
```

## Key Components

### 1. Factory (DynamicTableSourceFactory)
- Entry point for Flink's table API
- Validates configuration options
- Creates table source instances

### 2. Table Source (ScanTableSource / LookupTableSource)
- Defines how data is read
- Provides runtime providers (data stream or lookup function)
- Can implement additional capabilities (limit pushdown, projection pushdown, etc.)

### 3. Source (Flink 2.x Source API)
- **ConnectorSource**: Entry point, defines boundedness and creates reader/enumerator
- **ConnectorSourceReader**: Reads data from splits and converts to RowData format
- **ConnectorSplitEnumerator**: Discovers and assigns splits to readers
- **ConnectorSourceSplit**: Represents a portion of data (file, partition, offset range)
- Provides better support for checkpointing, split discovery, and watermarks

### 4. Lookup Function (LookupFunction) - Optional
- Implements lookup logic for dimension tables
- Used in temporal table joins

### 5. Type Conversion Utils
- Converts between source data format and Flink internal types
- Handles various data types (primitives, complex types, temporal types)

## Supported Capabilities

This template includes examples for:
- ✅ Scan table source (batch and streaming)
- ✅ Limit pushdown
- ✅ Lookup table source (optional, for joins)

You can add support for:
- Projection pushdown (`SupportsProjectionPushDown`)
- Filter pushdown (`SupportsFilterPushDown`)
- Partition pushdown (`SupportsPartitionPushDown`)
- Watermark pushdown (`SupportsWatermarkPushDown`)
- Source function with checkpointing

## Data Type Support

Update `TypeConversionUtils.java` to support these Flink types:
- Primitives: `BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`
- Strings: `CHAR`, `VARCHAR`, `STRING`
- Temporal: `DATE`, `TIME`, `TIMESTAMP`
- Complex: `ARRAY`, `MAP`, `ROW`, `MULTISET`
- Special: `DECIMAL`, `BINARY`, `VARBINARY`

## Best Practices

1. **Error Handling**: Implement proper error handling and logging
2. **Resource Management**: Always clean up resources in `close()` methods
3. **Serializability**: Ensure all functions are serializable (avoid non-serializable fields)
4. **Testing**: Write comprehensive unit and integration tests
5. **Documentation**: Document all configuration options and usage examples
6. **Validation**: Validate all user inputs in the factory
7. **Type Safety**: Use Flink's type system correctly for all data conversions

## Troubleshooting

### Service loader not finding factory
- Ensure `META-INF/services/org.apache.flink.table.factories.Factory` contains the correct fully qualified class name
- Check that the file has no extra whitespace or newlines
- Verify the JAR contains the service loader file in the correct location

### ClassNotFoundException
- Make sure all dependencies are correctly shaded in the JAR
- Check that `maven-shade-plugin` configuration is correct

### Type conversion errors
- Verify that your type conversion logic handles all data types correctly
- Check that null values are handled properly
- Ensure date/time formats match Flink's expectations

## Additional Resources

- [Flink Table API Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/overview/)
- [Flink Connector Development Guide](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sourcessinks/)
- [DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/)

## License

This template is derived from the flink-faker project.
Update with your own license as needed.
