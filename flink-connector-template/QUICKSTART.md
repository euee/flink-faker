# Quick Start Guide

This guide will help you quickly create a new Flink connector from this template.

## Step 1: Copy Template (2 minutes)

```bash
# Copy template to your new project directory
cp -r flink-connector-template my-flink-connector
cd my-flink-connector
```

## Step 2: Find & Replace (5 minutes)

Replace these placeholders throughout the project:

| Placeholder | Replace With | Example |
|------------|--------------|---------|
| `{{CONNECTOR_NAME}}` | Your connector identifier | `http`, `redis`, `mongodb` |
| `com.example.connector` | Your package name | `com.mycompany.flink.http` |
| `Connector` class prefix | Your connector name | `Http`, `Redis`, `MongoDB` |

**Quick find & replace commands:**

```bash
# Find all files with placeholders
grep -r "{{CONNECTOR_NAME}}" . --exclude-dir=target

# Replace {{CONNECTOR_NAME}} (macOS)
find . -type f -not -path "*/target/*" -exec sed -i '' 's/{{CONNECTOR_NAME}}/myconnector/g' {} +

# Replace {{CONNECTOR_NAME}} (Linux)
find . -type f -not -path "*/target/*" -exec sed -i 's/{{CONNECTOR_NAME}}/myconnector/g' {} +

# Replace package name (macOS)
find . -type f -name "*.java" -exec sed -i '' 's/com\.example\.connector/com.mycompany.flink.myconnector/g' {} +

# Replace package name (Linux)
find . -type f -name "*.java" -exec sed -i 's/com\.example\.connector/com.mycompany.flink.myconnector/g' {} +
```

**Manual steps:**
1. Rename package directories:
   ```bash
   # Create new package structure
   mkdir -p src/main/java/com/mycompany/flink/myconnector
   mkdir -p src/test/java/com/mycompany/flink/myconnector

   # Move files
   mv src/main/java/com/example/connector/* src/main/java/com/mycompany/flink/myconnector/
   mv src/test/java/com/example/connector/* src/test/java/com/mycompany/flink/myconnector/

   # Remove old directories
   rm -rf src/main/java/com/example
   rm -rf src/test/java/com/example
   ```

2. Rename Java classes (optional but recommended):
   ```bash
   # Example: Rename Connector* to Http*
   mv src/main/java/.../ConnectorTableSourceFactory.java .../HttpTableSourceFactory.java
   mv src/main/java/.../ConnectorTableSource.java .../HttpTableSource.java
   # ... etc for all classes
   ```

3. Update service loader file:
   ```bash
   # Edit src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory
   # Replace with your factory's fully qualified name:
   # com.mycompany.flink.myconnector.MyConnectorTableSourceFactory
   ```

## Step 3: Implement Connector Logic (varies)

### Minimal Implementation Checklist:

**Factory (ConnectorTableSourceFactory.java):**
- [ ] Update `IDENTIFIER` constant
- [ ] Define configuration options (e.g., endpoint, credentials)
- [ ] Implement `requiredOptions()` and `optionalOptions()`
- [ ] Add validation logic

**Source Function (ConnectorSourceFunction.java):**
- [ ] Add connector client field
- [ ] Initialize client in `open()` method
- [ ] Implement data fetching in `run()` method
- [ ] Implement type conversion in `convertToFlinkType()`
- [ ] Clean up in `close()` method

**Type Conversion (TypeConversionUtils.java):**
- [ ] Customize for your data format (JSON, Avro, etc.)
- [ ] Support all required data types

## Step 4: Update Dependencies (2 minutes)

Edit `pom.xml` and add your connector dependencies:

```xml
<dependencies>
  <!-- Your connector client library -->
  <dependency>
    <groupId>com.example</groupId>
    <artifactId>your-client</artifactId>
    <version>1.0.0</version>
  </dependency>

  <!-- Existing dependencies... -->
</dependencies>
```

## Step 5: Build & Test (5 minutes)

```bash
# Format code
mvn spotless:apply

# Compile
mvn clean compile

# Run tests
mvn test

# Build JAR
mvn clean package
```

The compiled JAR will be in `target/flink-connector-yourname-0.1.0.jar`

## Step 6: Use Your Connector (2 minutes)

1. Copy JAR to Flink's `lib/` directory
2. Start Flink cluster
3. Use in SQL:

```sql
CREATE TABLE my_source (
    id BIGINT,
    name STRING,
    value DOUBLE
) WITH (
    'connector' = 'myconnector',  -- Your IDENTIFIER from factory
    'endpoint' = 'http://localhost:8080',
    'batch-size' = '100'
);

SELECT * FROM my_source;
```

## Common Issues & Solutions

### Issue: "Could not find any factory for identifier 'myconnector'"

**Solution:**
1. Check service loader file exists: `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`
2. Verify it contains your factory's fully qualified name
3. Rebuild the JAR: `mvn clean package`

### Issue: ClassNotFoundException

**Solution:**
1. Ensure dependencies are shaded in pom.xml
2. Check maven-shade-plugin configuration
3. Rebuild: `mvn clean package`

### Issue: Type conversion errors

**Solution:**
1. Check `TypeConversionUtils.java` handles your data types
2. Verify null handling
3. Add logging to debug conversion issues

## Next Steps

- Read the full [README.md](README.md) for detailed documentation
- Implement additional capabilities (filter pushdown, projection pushdown)
- Add comprehensive tests
- Add support for lookup joins (if needed)
- Configure watermarks for event-time processing

## Example Implementations

For reference implementations, see:
- **flink-faker**: Mock data generation (this project)
- **flink-connector-jdbc**: Database connectivity
- **flink-connector-kafka**: Message queue integration
- **flink-connector-elasticsearch**: Search engine sink

Good luck building your Flink connector! 🚀
