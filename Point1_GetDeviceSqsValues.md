# Point 1: GetDeviceSqsValues Class Documentation

## Overview
The `GetDeviceSqsValues` class is designed to handle SQS (Simple Queue Service) message attributes for device processing operations. It provides two constructors for different initialization scenarios.

## Class Properties
- `LargestDeviceIdSeen`: String - Tracks the largest device ID processed
- `HasMoreData`: Boolean - Indicates if there's more data to process
- `CurrentServiceProviderId`: Integer - Current service provider identifier
- `RetryCount`: Integer - Number of retry attempts

## Constructors

### Default Constructor
```csharp
public GetDeviceSqsValues()
{
    LargestDeviceIdSeen = "0";
    HasMoreData = true;
    CurrentServiceProviderId = 0;
    RetryCount = 0;
}
```

**Purpose**: Initializes the class with default values for new processing sessions.

**Default Values**:
- `LargestDeviceIdSeen`: "0"
- `HasMoreData`: true
- `CurrentServiceProviderId`: 0
- `RetryCount`: 0

### SQS Message Constructor
```csharp
public GetDeviceSqsValues(KeySysLambdaContext context, SQSMessage message)
{
    // Implementation details below
}
```

**Purpose**: Initializes the class by extracting values from SQS message attributes.

**Parameters**:
- `context`: KeySysLambdaContext - Lambda execution context for logging
- `message`: SQSMessage - SQS message containing attributes

## SQS Message Attribute Processing

The constructor processes the following message attributes:

### LargestDeviceIdSeen
- **Attribute Key**: "LargestDeviceIdSeen"
- **Type**: String
- **Processing**: Direct string value assignment
- **Logging**: Logs the extracted value using `context.LogInfo`

### HasMoreData
- **Attribute Key**: "HasMoreData"
- **Type**: Boolean (stored as string in SQS)
- **Processing**: Converts string to lowercase and compares with "true"
- **Logging**: Logs the boolean value using `context.LogInfo`

### CurrentServiceProviderId
- **Attribute Key**: "CurrentServiceProviderId"
- **Type**: Integer (stored as string in SQS)
- **Processing**: Parses string to Int32
- **Logging**: Logs the integer value using `context.LogInfo`

### RetryCount
- **Attribute Key**: "RetryCount"
- **Type**: Integer (stored as string in SQS)
- **Processing**: Parses string to Int32
- **Logging**: Logs the integer value using `context.LogInfo`

## Usage Pattern

This class is typically used in AWS Lambda functions that process SQS messages for device management operations. The class provides a clean way to extract and validate message attributes while maintaining proper logging for debugging and monitoring purposes.

## Error Handling Notes

- The implementation uses `ContainsKey()` checks to safely access message attributes
- Missing attributes will not cause exceptions; properties will retain their default values
- Integer parsing uses `Int32.Parse()` which may throw exceptions for invalid formats