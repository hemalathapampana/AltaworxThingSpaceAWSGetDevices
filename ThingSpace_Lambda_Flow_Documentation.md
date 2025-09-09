# ThingSpace AWS Lambda Flow Documentation

## Overview
This document provides comprehensive flow documentation for the ThingSpace AWS Lambda functions, including high-level sequential flow and detailed low-level function analysis.

## High-Level Sequential Flow

### Main Lambda Function: AltaworxThingSpaceAWSGetDevices
The primary Lambda function that handles device retrieval from ThingSpace API and triggers device usage processing.

```
1. FunctionHandler (Entry Point)
   ↓
2. BaseFunctionHandler (Initialize Context)
   ↓
3. TryProcessDeviceList (Main Processing Logic)
   ↓
4. ProcessDeviceList (Device Processing Loop)
   ↓
5. GetThingSpaceDevices (API Calls & Data Retrieval)
   ↓
6. SqlBulkCopy (Data Storage)
   ↓
7. UpdateThingSpaceDevices (Database Updates)
   ↓
8. SendMessageToGetDeviceUsageQueue (Trigger Usage Processing)
```

## Detailed Low-Level Flow Analysis

### 1. Function Entry Point
**Function:** `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
**Location:** AltaworxThingSpaceAWSGetDevices.cs:43

#### Purpose:
- Main entry point for AWS Lambda execution
- Handles SQS event processing and initialization

#### Key Steps:
1. **Context Initialization**
   - Creates `KeySysLambdaContext` using `BaseFunctionHandler()`
   - Loads environment variables and configuration

2. **Environment Variable Setup**
   - `ThingSpaceDevicesGetURL`: API endpoint for device retrieval
   - `MaxCyclesToProcess`: Maximum processing cycles limit
   - `ThingSpaceDestinationQueueGetDevicesURL`: Queue URL for continuation
   - `DeviceUsageQueueURL`: Queue URL for device usage processing

3. **Record Processing**
   - Processes each SQS record if available
   - Extracts message queue values using `getMessageQueueValues()`
   - Calls `TryProcessDeviceList()` for each record

4. **Exception Handling**
   - Logs exceptions and ensures cleanup
   - Calls `CleanUp()` method in finally block

---

### 2. Base Function Handler (AwsFunctionBase)
**Function:** `BaseFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)`
**Location:** AwsFunctionBase.cs:40

#### Purpose:
- Initialize Lambda context with KeySys-specific functionality
- Set up logging and database connections

#### Key Steps:
1. **Context Creation**
   - Creates `KeySysLambdaContext` wrapper around AWS Lambda context
   - Enables OU-specific logic unless skipped

2. **Configuration Loading**
   - Loads organizational unit settings
   - Establishes database connection strings
   - Sets up logging infrastructure

---

### 3. Try Process Device List
**Function:** `TryProcessDeviceList(KeySysLambdaContext context, GetDeviceSqsValues sqsValues)`
**Location:** AltaworxThingSpaceAWSGetDevices.cs:92

#### Purpose:
- Orchestrates device list processing with error handling
- Manages service provider selection and staging table truncation

#### Key Steps:
1. **Service Provider Selection**
   - If `CurrentServiceProviderId` is 0, get next available provider
   - Uses `ServiceProviderCommon.GetNextServiceProviderId()` with ThingSpace integration type
   - Handles cases: 0 (exception), -1 (no auth record), positive (valid provider)

2. **Staging Table Cleanup**
   - Calls `TruncateThingSpaceDeviceAndUsageStagingWithPolicy()`
   - Clears previous staging data before new processing

3. **Main Processing**
   - Calls `ProcessDeviceList()` if proceed flag is true
   - Comprehensive exception handling with detailed logging

---

### 4. Process Device List (Main Processing Loop)
**Function:** `ProcessDeviceList(KeySysLambdaContext context, GetDeviceSqsValues sqsValues)`
**Location:** AltaworxThingSpaceAWSGetDevices.cs:133

#### Purpose:
- Main processing loop that retrieves devices from ThingSpace API
- Manages pagination, data transformation, and database operations

#### Key Steps:

##### 4.1 Device Retrieval Loop
- **Cycle Management**: Loops while `CycleCounter <= MaxCyclesToProcess`
- **API Calls**: Calls `GetThingSpaceDevices()` for each cycle
- **Pagination**: Continues until `isLastCycle` becomes true

##### 4.2 Exception Handling Strategy
- **EXCEPTION Messages**: Retry up to `MAX_RETRY_COUNT` (3), then requeue
- **EXPIRED Messages**: Requeue immediately for credential renewal
- **Other Exceptions**: Considered critical, no retry

##### 4.3 Data Table Construction
Creates DataTable with columns:
- ID, ICCID, IMSI, MSISDN, IMEI
- Status, RatePlan, AccountNumber
- CreatedBy, DeviceCreatedDate, LastActivationDate, LastUsageDate
- BillingCycleEndDate, ServiceProviderId, CreatedDate
- Primary Place of Use fields (FirstName, MiddleName, LastName)
- ThingSpacePPU (JSON), IPAddress

##### 4.4 Billing Period Calculation
- Uses `ThingSpaceCommon.GetBillingPeriod()` with UTC timezone
- Calculates billing cycle end date for current service provider

##### 4.5 Data Transformation
- Processes each device using `AddToDataRow()`
- Extracts device identifiers, carrier information, and extended attributes
- Serializes Primary Place of Use information to JSON

##### 4.6 Database Operations
- **Bulk Insert**: Uses `SqlBulkCopy()` to insert staging data
- **Device Updates**: Calls `UpdateThingSpaceDevicesWithPolicy()`
- **Usage Queue**: Triggers `SendMessageToGetDeviceUsageQueue()`

##### 4.7 Service Provider Continuation
- Gets next service provider using `ServiceProviderCommon.GetNextServiceProviderId()`
- Sets up continuation parameters if more providers exist
- Requeues message for next service provider processing

---

### 5. Get ThingSpace Devices (API Integration)
**Function:** `GetThingSpaceDevices(KeySysLambdaContext context, GetDeviceSqsValues sqsValues)`
**Location:** AltaworxThingSpaceAWSGetDevices.cs:299

#### Purpose:
- Handles ThingSpace API authentication and device retrieval
- Manages pagination and account number resolution

#### Key Steps:

##### 5.1 Authentication Setup
- **Get Auth Info**: `ThingSpaceCommon.GetThingspaceAuthenticationInformation()`
- **Access Token**: `ThingSpaceCommon.GetAccessToken()` using client credentials
- **Session Token**: `ThingSpaceCommon.GetSessionToken()` using username/password
- **Token Validation**: Checks if token expires within 60 seconds

##### 5.2 Account Number Resolution
- Checks if account number exists in authentication record
- If missing, calls `ThingSpaceCommon.GetAccountNumber()` to retrieve from API
- Updates authentication record with discovered account number

##### 5.3 API Request Construction
- **Security Protocol**: Sets TLS 1.1, 1.2, 1.3 support
- **Headers**: Authorization (Bearer token), Accept (application/json), VZ-M2M-Token
- **Request Body**: JSON with accountName and largestDeviceIdSeen for pagination

##### 5.4 Response Processing
- **Success Handling**: Deserializes response to `ThingSpaceDeviceResponseRootObject`
- **Device Processing**: Adds devices to `sqsValues.ThingSpaceDeviceList`
- **Pagination Management**: 
  - Extracts `hasMoreData` flag from response
  - Gets `LargestDeviceIdSeen` from DeviceId extended attribute of last device
  - Sets `isLastCycle` based on pagination status

##### 5.5 Error Handling
- **API Failures**: Logs error response and throws exception with "EXCEPTION" prefix
- **Authentication Failures**: Handles missing tokens with specific error messages
- **Token Expiration**: Throws "EXPIRED" exception for token renewal

---

### 6. Supporting Functions Analysis

#### 6.1 ServiceProviderCommon Class
**Location:** ServiceProviderCommon.cs

##### GetNextServiceProviderId
**Purpose:** Retrieves next service provider for processing
- **Parameters:** connectionString, integrationType, currentServiceProviderId
- **Returns:** Next provider ID (0=exception, -1=no auth record, >0=valid)
- **SQL:** Calls `usp_DeviceSync_Get_NextServiceProviderIdByIntegration`

##### GetServiceProvider
**Purpose:** Retrieves service provider configuration
- **Returns:** ServiceProvider object with billing period settings
- **Key Fields:** BillPeriodEndDay, BillPeriodEndHour, OptimizationStartHour

#### 6.2 ThingSpaceCommon Class
**Location:** ThingSpaceCommon.cs

##### GetAccessToken
**Purpose:** OAuth2 client credentials flow for API access
- **Authentication:** Base64 encoded client_id:client_secret
- **Request:** POST to auth token URL with grant_type=client_credentials
- **Returns:** `ThingSpaceTokenResponse` with access token and expiration

##### GetSessionToken
**Purpose:** Gets session token using username/password
- **Authentication:** Bearer token from access token
- **Request:** POST to auth URL with username/password JSON
- **Returns:** `ThingSpaceLoginResponse` with session token

##### GetAccountNumber
**Purpose:** Discovers account number from API when not configured
- **Request:** POST to devices endpoint with active device filter
- **Returns:** Account name from first device response

##### GetBillingPeriod
**Purpose:** Calculates billing period for service provider
- **Logic:** 
  - Gets billing end day/hour from service provider settings
  - Uses defaults (day 23, hour 0) if not configured
  - Adjusts for current month vs next month based on current date
  - Calls `BillingPeriodHelper.GetBillingPeriodForServiceProviderByCurrentDate()`

#### 6.3 AwsFunctionBase Class
**Location:** AwsFunctionBase.cs

##### Key Utility Functions:
- **LogInfo:** Centralized logging with caller information
- **SqlBulkCopy:** Bulk database operations with retry logic
- **AwsCredentials:** AWS credential management from encrypted settings
- **GetInstance/GetQueue:** Database retrieval for optimization records
- **GetFANFilter:** Service provider specific filtering logic

#### 6.4 RetryPolicyHelper Class
**Location:** RetryPolicyHelper.cs

##### GetSqlTransientPolicy
**Purpose:** Provides resilient SQL operations with retry logic
- **Retry Count:** 3 attempts by default
- **Fallback:** Captures error messages for logging
- **Usage:** Wraps database operations for transient failure handling

---

### 7. Database Operations Flow

#### 7.1 Staging Table Operations
1. **Truncation:** `usp_ThingSpace_Truncate_DeviceAndUsageStaging`
2. **Bulk Insert:** SqlBulkCopy to `ThingSpaceDeviceStaging`
3. **Device Update:** `usp_ThingSpace_Update_Device`
4. **Detail Update:** `usp_ThingSpace_Update_DeviceDetail`

#### 7.2 Authentication Updates
- **Account Number Update:** `usp_ThingSpace_Update_AuthenticationAccountNumber`

---

### 8. Message Queue Operations

#### 8.1 Continuation Queue (GetDevices)
**Purpose:** Continue device processing for pagination or next service provider
**Attributes:**
- HasMoreData: Boolean flag for pagination
- LargestDeviceIdSeen: Pagination cursor
- CurrentServiceProviderId: Current provider being processed
- RetryCount: Number of retry attempts

#### 8.2 Device Usage Queue
**Purpose:** Trigger device usage data collection after device sync
**Attributes:**
- InitializeProcessing: Boolean flag to start usage processing
- GroupNumber: Processing group (starts at 0)
- ServiceProviderId: Provider for usage collection

---

## Error Handling Strategy

### 1. Exception Categories
- **EXCEPTION:** Retriable errors (max 3 retries)
- **EXPIRED:** Token expiration (immediate retry with new credentials)
- **Critical:** Non-retriable errors (processing stops)

### 2. Retry Mechanisms
- **SQS Requeuing:** For retriable and expired errors
- **SQL Retry Policy:** Transient database error handling
- **HTTP Retry Policy:** API call resilience

### 3. Logging Strategy
- **Structured Logging:** Type, message, caller information
- **Exception Details:** Stack traces and context
- **Performance Metrics:** Processing counts and timing

---

## Configuration Requirements

### Environment Variables
- `ThingSpaceDevicesGetURL`: API endpoint for device retrieval
- `MaxCyclesToProcess`: Maximum processing cycles (default: varies)
- `ThingSpaceDestinationQueueGetDevicesURL`: Continuation queue URL
- `DeviceUsageQueueURL`: Usage processing queue URL

### Database Requirements
- Central database connection for service provider and authentication data
- Staging tables: `ThingSpaceDeviceStaging`
- Stored procedures: Multiple `usp_ThingSpace_*` procedures

### ThingSpace API Configuration
- Client ID and Secret for OAuth2
- Username and Password for session authentication
- Base URL for API endpoints
- Account Number (auto-discovered if not configured)

---

## Performance Considerations

### 1. Pagination Strategy
- Uses `largestDeviceIdSeen` for efficient pagination
- Processes devices in batches based on API response limits
- Continues until `hasMoreData` flag becomes false

### 2. Bulk Operations
- SqlBulkCopy for efficient database inserts
- Batch size configuration through SQL constants
- Timeout management for large datasets

### 3. Queue Management
- 5-second delay for continuation messages
- Separate queues for device sync and usage processing
- Retry count tracking to prevent infinite loops

### 4. Resource Management
- Proper disposal of database connections and HTTP clients
- Context cleanup in finally blocks
- Memory efficient processing of large device lists

---

## Security Considerations

### 1. Credential Management
- Base64 encoded secrets in configuration
- OAuth2 client credentials flow
- Secure token handling and expiration management

### 2. Database Security
- Parameterized queries to prevent SQL injection
- Connection string encryption
- Stored procedure usage for data operations

### 3. API Security
- TLS 1.1+ enforcement for all HTTP communications
- Bearer token authentication
- Session token management for API calls

---

## Monitoring and Observability

### 1. Logging Points
- Function entry/exit with timing
- API call success/failure with response codes
- Database operation results
- Queue message processing status

### 2. Key Metrics
- Device processing counts per cycle
- API response times and success rates
- Database operation performance
- Queue message processing latency

### 3. Error Tracking
- Exception categorization and retry counts
- Failed API calls with error details
- Database connection and query failures
- Queue processing errors and recovery
