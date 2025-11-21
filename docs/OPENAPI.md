# OpenAPI Specification and Stub Generation

## Overview

This project uses an external OpenAPI specification file (`src/main/resources/openapi.yaml`) to define the REST API. The specification is used for:

1. **API Documentation**: Served via Swagger UI at `/swagger-ui.html`
2. **API Contract**: Defines the interface contracts for all REST endpoints
3. **Code Generation**: Generates Kotlin Spring server stubs automatically during build

## OpenAPI Specification File

The OpenAPI spec is located at:
```
src/main/resources/openapi.yaml
```

This YAML file contains:
- Complete API endpoint definitions
- Request/response schemas
- Data models
- API documentation and descriptions

## Stub Generation

The project uses the [OpenAPI Generator](https://openapi-generator.tech/) Gradle plugin to automatically generate server stubs during the build process.

### Generated Code Location

Generated interfaces and models are placed in:
```
build/generated/openapi/src/main/kotlin/
```

The generated code includes:
- API interfaces in `com.dragos.kafkainspector.api.generated` package
- Data models in `com.dragos.kafkainspector.model.generated` package

### Build Configuration

The OpenAPI Generator is configured in `build.gradle.kts`:

```kotlin
openApiGenerate {
    generatorName.set("kotlin-spring")
    inputSpec.set("$rootDir/src/main/resources/openapi.yaml")
    outputDir.set("${layout.buildDirectory.get()}/generated/openapi")
    apiPackage.set("com.dragos.kafkainspector.api.generated")
    modelPackage.set("com.dragos.kafkainspector.model.generated")
    configOptions.set(
        mapOf(
            "interfaceOnly" to "true",
            "skipDefaultInterface" to "true",
            "useTags" to "true",
            "useSpringBoot3" to "true",
            "serializationLibrary" to "jackson",
        ),
    )
}
```

### Build Process

The stub generation is automatically triggered during the build:

```bash
./gradlew clean build
```

The `openApiGenerate` task runs before `compileKotlin`, ensuring stubs are available for compilation.

### Generated Code Exclusions

Generated code is:
- Excluded from ktlint checks
- Not committed to version control (located in `build/` directory)
- Regenerated on each clean build

## Modifying the API

To modify the API:

1. **Edit the OpenAPI spec**: Update `src/main/resources/openapi.yaml`
2. **Rebuild the project**: Run `./gradlew clean build`
3. **Verify**: Generated stubs will be updated automatically

## Viewing the API Documentation

Access the interactive API documentation:

1. **Start the application**: `./gradlew bootRun` or `java -jar build/libs/kafka-dlq-inspector-0.1.0-all.jar`
2. **Open Swagger UI**: Navigate to `http://localhost:8080/swagger-ui.html`
3. **View OpenAPI JSON**: Access `http://localhost:8080/v3/api-docs`

## Benefits of External OpenAPI Spec

1. **Single Source of Truth**: API contract is defined in one place
2. **Contract-First Development**: Design the API before implementing
3. **Automatic Documentation**: API docs are always up-to-date
4. **Code Generation**: Reduces boilerplate and ensures consistency
5. **Client Generation**: Can generate clients in multiple languages
6. **Version Control**: Easy to track API changes over time
