# ThingSpace AWS Get Devices Lambda Function - Flow Documentation

## Overview
The `AltaworxThingSpaceAWSGetDevices` Lambda function is responsible for retrieving device information from ThingSpace API and processing it for storage in the database. This document provides a comprehensive high-level and low-level flow analysis of the function and its supporting classes.

---

## High-Level Sequential Flow

### 1. Main Function Entry Point
- **Function**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
- **Purpose**: Entry point for Lambda execution triggered by SQS events

### 2. Base Function Initialization
- **Function**: `BaseFunctionHandler(context)`
- **Purpose**: Initialize KeySys Lambda context and settings

### 3. Environment Variable Setup
- **Purpose**: Load configuration from environment variables or context
- **Variables**: ThingSpaceDevicesGetURL, MaxCyclesToProcess, Queue URLs

### 4. SQS Message Processing
- **Function**: `getMessageQueueValues()` → `TryProcessDeviceList()`
- **Purpose**: Parse SQS messages and initiate device processing

### 5. Service Provider Management
- **Function**: `ServiceProviderCommon.GetNextServiceProviderId()`
- **Purpose**: Get next service provider to process

### 6. Device List Processing
- **Function**: `ProcessDeviceList()`
- **Purpose**: Main processing loop for device retrieval and storage

### 7. ThingSpace API Integration
- **Function**: `GetThingSpaceDevices()`
- **Purpose**: Retrieve devices from ThingSpace API with pagination

### 8. Data Transformation and Storage
- **Function**: `AddToDataRow()` → `SqlBulkCopy()`
- **Purpose**: Transform API data and bulk insert into database

### 9. Database Updates
- **Function**: `UpdateThingSpaceDevicesWithPolicy()`
- **Purpose**: Execute stored procedures to update device data

### 10. Queue Management
- **Function**: `SendMessageToQueue()` / `SendMessageToGetDeviceUsageQueue()`
- **Purpose**: Send messages to continuation or usage processing queues

### 11. Cleanup
- **Function**: `CleanUp()`
- **Purpose**: Clean up resources and context

---

## Low-Level Flow Documentation

## 1. AltaworxThingSpaceAWSGetDevices.cs

### Class: Function : AwsFunctionBase

#### Key Methods:

##### FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
**Purpose**: Main entry point for Lambda execution
**Flow**:
- Initialize KeySysLambdaContext via `BaseFunctionHandler()`
- Load environment variables (ThingSpaceDevicesGetURL, MaxCyclesToProcess, etc.)
- Process SQS records or execute direct processing
- Handle exceptions and perform cleanup
- **Error Handling**: Try-catch with logging and cleanup

##### TryProcessDeviceList(KeySysLambdaContext context, GetDeviceSqsValues sqsValues)
**Purpose**: Wrapper for device processing with service provider validation
**Flow**:
- Check if CurrentServiceProviderId is set (0 = initial value)
- Call `ServiceProviderCommon.GetNextServiceProviderId()` if needed
- Handle service provider validation results:
  - 0: Exception occurred
  - -1: No authentication record found
  - >0: Valid service provider, proceed with processing
- Truncate staging tables if starting new service provider
- Call `ProcessDeviceList()` if validation passes
- **Error Handling**: Exception logging with context

##### ProcessDeviceList(KeySysLambdaContext context, GetDeviceSqsValues sqsValues)
**Purpose**: Main device processing loop with retry logic
**Flow**:
- Initialize cycle tracking and DataTable structure
- Execute processing loop while `CycleCounter <= MaxCyclesToProcess`
- Call `GetThingSpaceDevices()` for each cycle
- Handle pagination with `isLastCycle` flag
- **Retry Logic**:
  - EXCEPTION messages: Retry up to MAX_RETRY_COUNT (3)
  - EXPIRED messages: Requeue for credential renewal
  - Other exceptions: No retry, considered critical
- Transform device data using `AddToDataRow()`
- Perform bulk insert with `SqlBulkCopy()`
- Execute database updates via `UpdateThingSpaceDevicesWithPolicy()`
- Handle service provider continuation logic
- Queue next processing messages

##### GetThingSpaceDevices(KeySysLambdaContext context, GetDeviceSqsValues sqsValues)
**Purpose**: Retrieve devices from ThingSpace API with authentication
**Flow**:
- Get authentication information via `ThingSpaceCommon.GetThingspaceAuthenticationInformation()`
- Validate authentication exists
- Get access token via `ThingSpaceCommon.GetAccessToken()`
- Check token expiration (EXPIRED_TIME = 60 seconds)
- Get session token via `ThingSpaceCommon.GetSessionToken()`
- Retrieve/validate account number if needed
- **HTTP Client Setup**:
  - Set security protocols (TLS 1.1-1.3)
  - Configure headers: Authorization, Accept, VZ-M2M-Token
  - Create JSON request body with accountName and largestDeviceIdSeen
- **API Call Processing**:
  - POST request to ThingSpaceDevicesGetURL
  - Deserialize response to `ThingSpaceDeviceResponseRootObject`
  - Process device list and pagination flags
  - Update `HasMoreData` and `LargestDeviceIdSeen` from last record
- **Error Handling**: Status code validation with detailed error logging

##### Database Operations
- **SetThingspaceAuthenticationAccountNumber()**: Update account number in authentication table
- **TruncateThingSpaceDeviceAndUsageStagingWithPolicy()**: Clear staging tables with retry policy
- **UpdateThingSpaceDevicesWithPolicy()**: Execute device update stored procedures with retry

##### Queue Operations
- **SendMessageToQueue()**: Send continuation messages to device processing queue
- **SendMessageToGetDeviceUsageQueue()**: Trigger device usage processing

---

## 2. AwsFunctionBase.cs

### Class: AwsFunctionBase

#### Key Methods:

##### BaseFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)
**Purpose**: Initialize Lambda context with settings
**Flow**:
- Create `KeySysLambdaContext` instance
- Load OU-specific settings unless skipped
- Return initialized context

##### LogInfo(KeySysLambdaContext context, string desc, object detail = null, ...)
**Purpose**: Centralized logging with caller information
**Flow**:
- Format log message with caller file, line, and function name
- Call context.LogInfo() with formatted message

##### SqlBulkCopy(KeySysLambdaContext context, string connectionString, DataTable table, string tableName, ...)
**Purpose**: Perform bulk database operations with error handling
**Flow**:
- Open SQL connection
- Configure SqlBulkCopy with timeout and batch size
- Apply column mappings if provided
- Execute bulk insert operation
- **Error Handling**: SQL exceptions, connection issues, and general exceptions

##### AWS Credentials Management
- **AwsCredentials()**: Create AWS credentials from context settings
- **AwsSesCredentials()**: Create SES-specific AWS credentials
- Base64 decode secret access keys

##### Database Helper Methods
- **GetCustomerName()**: Retrieve customer name by ID
- **GetInstance()**: Get optimization instance details
- **GetQueue()**: Retrieve optimization queue information
- **GetCommGroups()**: Get communication groups for instance

---

## 3. RetryPolicyHelper.cs

### Class: RetryPolicyHelper (Static)

#### Key Methods:

##### GetSqlTransientPolicy(IKeysysLogger logger, List<string> errorMessages, int retryCount = 3)
**Purpose**: Create retry policy for SQL transient errors
**Flow**:
- Create fallback policy for error message collection
- Wrap SQL retry policy with fallback
- Return combined policy

##### PollyRetryHttpRequestAsync(IKeysysLogger logger = null, int numberOfRetry = 3)
**Purpose**: Create async retry policy for HTTP requests
**Flow**:
- **Retry Policy**:
  - Handle exceptions and unsuccessful HTTP responses
  - Use exponential backoff: `Math.Pow(API_ERROR_DELAY_IN_SECONDS, retryAttempt)`
  - Log retry attempts with wait time
- **Fallback Policy**:
  - Create custom error response for exceptions
  - Return original response for HTTP failures
- Return wrapped policy

##### CalculateRetryDelay(int retryAttempt)
**Purpose**: Calculate exponential backoff delay
**Formula**: `TimeSpan.FromSeconds(Math.Pow(CommonConstants.API_ERROR_DELAY_IN_SECONDS, retryAttempt))`

##### Specialized Retry Policies
- **PollyRetryRevIOHttpRequestAsync()**: Handle RevIO-specific responses (429 Too Many Requests)
- **PollyRetryForProxyRequestAsync()**: Handle proxy request retries
- **PollyRetryForSQSMessage()**: Handle SQS message retry logic

---

## 4. ServiceProviderCommon.cs

### Class: ServiceProviderCommon (Static)

#### Key Methods:

##### GetNextServiceProviderId(string connectionString, IntegrationType integrationType, int currentServiceProviderId)
**Purpose**: Retrieve next service provider for processing
**Flow**:
- Execute stored procedure `usp_DeviceSync_Get_NextServiceProviderIdByIntegration`
- Pass current provider ID and integration type (ThingSpace)
- **Return Values**:
  - 0: Exception occurred
  - -1: No authentication record found
  - >0: Valid next service provider ID
- **Error Handling**: Exception logging to Debug output

##### GetServiceProvider(string connectionString, int serviceProviderId)
**Purpose**: Retrieve service provider details by ID
**Flow**:
- Query ServiceProvider table with comprehensive field selection
- Map database fields to ServiceProvider object
- Handle nullable fields (TenantId, BillPeriodEndDay, etc.)
- Return null if no provider found

##### GetServiceProviders(string connectionString)
**Purpose**: Retrieve all service providers
**Flow**:
- Query all records from ServiceProvider table
- Build list of ServiceProvider objects
- Return complete list

##### GetServiceProviderByName(string connectionString, string serviceProviderName)
**Purpose**: Retrieve service provider by name
**Flow**:
- Query ServiceProvider table by Name field
- Use ServiceProviderFromReader() for object mapping
- Return single ServiceProvider or null

---

## 5. ThingSpaceCommon.cs

### Class: ThingSpaceCommon (Static)

#### Key Methods:

##### GetAccessToken(ThingSpaceAuthentication thingSpaceAuth)
**Purpose**: Obtain OAuth access token from ThingSpace
**Flow**:
- Set security protocols (TLS 1.1-1.3)
- Create HTTP client with LambdaLoggingHandler
- Encode client credentials using Base64
- Set Authorization header with Basic auth
- **Request Configuration**:
  - Content-Type: application/x-www-form-urlencoded
  - Body: grant_type=client_credentials
- POST to AuthTokenUrl
- Deserialize response to `ThingSpaceTokenResponse`
- **Error Handling**: Return null on failure

##### GetSessionToken(ThingSpaceAuthentication thingSpaceAuth, ThingSpaceTokenResponse accessToken)
**Purpose**: Obtain session token for API calls
**Flow**:
- Set security protocols and create HTTP client
- Set Bearer token authorization header
- Decode password from Base64
- **Request Configuration**:
  - Content-Type: application/json
  - Body: JSON with username and decoded password
- POST to AuthUrl
- Deserialize response to `ThingSpaceLoginResponse`

##### GetAccountNumber(ThingSpaceAuthentication thingSpaceAuth, ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, string devicesGetUrl)
**Purpose**: Retrieve account number from device list
**Flow**:
- Configure HTTP client with Bearer token and session token
- **Request Configuration**:
  - Headers: Authorization, Accept, VZ-M2M-Token
  - Body: JSON with currentState: "Active"
- POST to devicesGetUrl
- Extract account number from first device in response
- Return null if no devices found

##### GetThingspaceAuthenticationInformation(string connectionString, int currentServiceProviderId)
**Purpose**: Retrieve authentication details from database
**Flow**:
- Execute stored procedure `usp_ThingSpace_Get_AuthenticationByProviderId`
- Map database fields to `ThingSpaceAuthentication` object
- Include all authentication parameters (URLs, credentials, etc.)
- **Error Handling**: Return null on exception

##### GetBillingPeriod(KeySysLambdaContext context, int serviceProviderId, DateTime currentDateTime, TimeZoneInfo timeZoneInfo)
**Purpose**: Calculate billing period for service provider
**Flow**:
- Get service provider details via `ServiceProviderCommon.GetServiceProvider()`
- Extract billing cycle configuration (end day/hour) with defaults
- Create initial `BillingPeriod` object
- **Logic**: If billing end day < current day, add 1 month
- Check for existing billing period in database
- Return existing or newly created billing period

##### Async Device Operations
- **GetThingSpaceDeviceAsync()**: Retrieve single device by ICCID
- **PutUpdateIdentifierAsync()**: Update device identifier
- **GetStatusRequest()**: Check request status

---

## Error Handling Strategies

### 1. Retry Mechanisms
- **SQL Transient Errors**: 3 retry attempts with exponential backoff
- **HTTP Requests**: Configurable retry count with exponential backoff
- **ThingSpace API**: Custom retry logic for token expiration and rate limiting

### 2. Exception Categories
- **EXCEPTION**: Retryable errors (up to 3 attempts)
- **EXPIRED**: Token expiration (immediate requeue)
- **Critical**: Non-retryable errors (fail immediately)

### 3. Fallback Strategies
- **Service Provider Continuation**: Process next available provider on current failure
- **Queue Management**: Requeue messages for retry or continuation
- **Database Resilience**: Retry policies for transient SQL errors

---

## Data Flow Summary

1. **Input**: SQS messages containing processing state
2. **Authentication**: Multi-step ThingSpace API authentication
3. **Device Retrieval**: Paginated API calls with continuation logic
4. **Data Transformation**: API response to database schema mapping
5. **Storage**: Bulk insert to staging tables
6. **Processing**: Stored procedure execution for final data updates
7. **Continuation**: Queue management for next processing cycles
8. **Output**: Updated device records and usage processing triggers

---

## Configuration Dependencies

### Environment Variables
- `ThingSpaceDevicesGetURL`: API endpoint for device retrieval
- `MaxCyclesToProcess`: Maximum processing cycles per execution
- `ThingSpaceDestinationQueueGetDevicesURL`: Continuation queue URL
- `DeviceUsageQueueURL`: Usage processing queue URL

### Database Dependencies
- **Stored Procedures**: Authentication, device updates, staging operations
- **Tables**: ServiceProvider, ThingSpaceDeviceStaging, device tables
- **Connection Strings**: Central database connectivity

### External APIs
- **ThingSpace API**: Device data source with OAuth2 authentication
- **AWS SQS**: Message queuing for processing coordination
- **SQL Server**: Data persistence and processing