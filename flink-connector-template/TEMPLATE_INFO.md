# Flink Connector Template

## What is this?

This is a reusable template for creating Apache Flink table connectors. It was extracted from the **flink-faker** project to help developers quickly build new Flink connectors.

## What's included?

This template provides a complete, working scaffold for a Flink connector including:

### Implementation
- ✅ **Factory** - Entry point for Flink's table API with configuration validation
- ✅ **Table Source** - Implementation of ScanTableSource with limit pushdown
- ✅ **Source Function** - Data reading/generation logic with resource management
- ✅ **Lookup Function** - Optional support for temporal table joins
- ✅ **Type Conversion Utils** - Utilities for converting data to Flink internal types

### Tests
- ✅ **Factory Unit Tests** - Configuration validation and factory creation tests
- ✅ **Source Function Unit Tests** - Data generation and type conversion tests
- ✅ **Integration Tests** - End-to-end tests with Flink SQL

### Configuration
- ✅ **Maven POM** - Build configuration with Flink dependencies, shade plugin, and code formatting
- ✅ **Service Loader** - META-INF configuration for connector discovery
- ✅ **Logging** - Test logging configuration
- ✅ **Git Ignore** - Standard .gitignore for Maven/IntelliJ projects

### Documentation
- ✅ **README** - Comprehensive documentation
- ✅ **Quick Start** - Step-by-step guide to create a connector in minutes
- ✅ **Inline TODOs** - Every file has clear TODO comments explaining what to customize

## How to use this template?

### Option 1: Quick Start (15-30 minutes)
Follow the [QUICKSTART.md](QUICKSTART.md) guide for a step-by-step walkthrough.

### Option 2: Detailed Implementation (1-2 hours)
Read the full [README.md](README.md) for comprehensive documentation and best practices.

## Who is this for?

This template is for developers who need to:
- Connect Flink to a custom data source
- Create a Flink table source for an external system
- Build a connector for a proprietary system or API
- Learn how Flink connectors work

## What types of connectors can I build?

This template supports building:

- **Data Sources** (ScanTableSource)
  - Batch sources (bounded data)
  - Streaming sources (unbounded data)
  - Sources with rate limiting
  - Sources with limit pushdown

- **Lookup Sources** (LookupTableSource)
  - Dimension tables for temporal joins
  - Key-based lookups

- **Extended Capabilities** (add as needed)
  - Filter pushdown
  - Projection pushdown
  - Partition pushdown
  - Watermark strategies

## Template Features

### Well-structured Code
- Clear separation of concerns
- Follows Flink connector best practices
- Proper resource management
- Comprehensive error handling examples

### Comprehensive Documentation
- Every class has detailed JavaDoc
- TODO comments guide implementation
- Real-world usage examples
- Troubleshooting guide

### Production-ready Testing
- Unit tests for each component
- Integration tests with Flink SQL
- Test utilities and helpers
- Examples for all common scenarios

### Build Configuration
- Maven setup with all required Flink dependencies
- Shade plugin for creating fat JARs
- Code formatting with Spotless
- Test configuration

## Prerequisites

To use this template, you should have:
- Java 11 or later
- Apache Maven 3.x
- Basic understanding of Apache Flink
- Familiarity with your data source/API

## Quick Example

After customizing the template, you can use your connector like this:

```sql
CREATE TABLE my_custom_source (
    id BIGINT,
    name STRING,
    timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'myconnector',
    'endpoint' = 'https://api.example.com',
    'api-key' = 'secret'
);

SELECT * FROM my_custom_source WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│              Flink SQL / Table API                      │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│         ConnectorTableSourceFactory                     │
│  - Validates configuration                              │
│  - Creates table source instance                        │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│            ConnectorTableSource                         │
│  - Defines changelog mode                               │
│  - Provides runtime provider                            │
│  - Implements capabilities (limit, etc.)                │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│         ConnectorSourceFunction                         │
│  - Opens connection to data source                      │
│  - Reads/generates data                                 │
│  - Converts to Flink RowData                            │
│  - Manages resources                                    │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              Your Data Source                           │
│  (Database, API, File System, Message Queue, etc.)     │
└─────────────────────────────────────────────────────────┘
```

## Template Origin

This template was extracted from the [flink-faker](https://github.com/knaufk/flink-faker) project, which is a production-quality Flink connector for generating fake data using the DataFaker library.

## License

This template maintains the same license as the flink-faker project (Apache 2.0).
You can customize it for your own connector with your own license.

## Support

For questions about:
- **This template**: Review the README.md and QUICKSTART.md
- **Flink connectors**: See [Flink documentation](https://nightlies.apache.org/flink/flink-docs-release-1.17/)
- **flink-faker project**: See the [original repository](https://github.com/knaufk/flink-faker)

## Contributing

If you improve this template or find issues, consider contributing back to the flink-faker project!

---

**Ready to build your connector?** Start with [QUICKSTART.md](QUICKSTART.md) 🚀
