# ThingSpace AWS Lambda Function - Comprehensive Analysis

## Table of Contents
1. [SQS Message Publisher Analysis](#1-sqs-message-publisher-analysis)
2. [Batch Size Configuration](#2-batch-size-configuration)
3. [ThingSpace API Integration](#3-thingspace-api-integration)
4. [Delayed Message Handling](#4-delayed-message-handling)
5. [Failed ICCID Logging](#5-failed-iccid-logging)
6. [Retry Configuration](#6-retry-configuration)
7. [Partially Unprocessed Groups](#7-partially-unprocessed-groups)

---

## 1. SQS Message Publisher Analysis

### Who publishes the first SQS message to ThingSpaceDeviceQueueURL?

**Answer:** The first SQS message to `ThingSpaceDeviceQueueURL` is published by **external triggers or schedulers** outside of this codebase.

### Evidence from Code Analysis:

The `AltaworxThingSpaceAWSGetDeviceUsage` Lambda function **receives** SQS messages but does not publish the initial message:

```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:40-41
public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
{
    KeySysLambdaContext keysysContext = null;
    try
    {
        keysysContext = BaseFunctionHandler(context);
        
        // Validates that SQS event exists
        if (sqsEvent == null || sqsEvent.Records == null || sqsEvent.Records.Count == 0)
        {
            LogInfo(keysysContext, "EXCEPTION", "This lambda can only be run from an upstream process.");
        }
```

**Simplified Process:**
1. External system sends SQS message → Lambda function receives it → Validates message exists → Processes or logs error

### SQS Message Structure and Attributes

The `GetDeviceUsageSqsValues` class handles SQS message attributes:

```csharp
// GetDeviceUsageSqsValues constructor from SQS message
public GetDeviceSqsValues(KeySysLambdaContext context, SQSMessage message)
{
    if (message.MessageAttributes.ContainsKey("LargestDeviceIdSeen"))
    {
        LargestDeviceIdSeen = message.MessageAttributes["LargestDeviceIdSeen"].StringValue;
        context.LogInfo("LargestDeviceIdSeen", LargestDeviceIdSeen);
    }

    if (message.MessageAttributes.ContainsKey("HasMoreData"))
    {
        HasMoreData = message.MessageAttributes["HasMoreData"].StringValue.ToLower() == "true";
        context.LogInfo("HasMoreData", HasMoreData);
    }

    if (message.MessageAttributes.ContainsKey("CurrentServiceProviderId"))
    {
        CurrentServiceProviderId = Int32.Parse(message.MessageAttributes["CurrentServiceProviderId"].StringValue);
        context.LogInfo("CurrentServiceProviderId", CurrentServiceProviderId);
    }

    if (message.MessageAttributes.ContainsKey("RetryCount"))
    {
        RetryCount = Int32.Parse(message.MessageAttributes["RetryCount"].StringValue);
        context.LogInfo("RetryCount", RetryCount);
    }
}
```

**Simplified Process:**
1. Check if attribute exists in message → Extract string value → Convert to appropriate type → Log the value

### Message Queue Value Extraction

```csharp
// GetDeviceUsageSqsValues constructor from SQS message
private GetDeviceUsageSqsValues GetMessageQueueValues(KeySysLambdaContext context, SQSMessage message)
{
    return new GetDeviceUsageSqsValues(context, message);
}
```

**Simplified Process:**
1. Receive SQS message → Create new GetDeviceUsageSqsValues object → Parse all attributes → Return structured object

### SQS Message Attributes Reference

| Attribute | Type | Purpose |
|-----------|------|---------|
| `LargestDeviceIdSeen` | String | Tracks pagination for device retrieval |
| `HasMoreData` | Boolean | Indicates if more devices need processing |
| `CurrentServiceProviderId` | Integer | Identifies the service provider being processed |
| `RetryCount` | Integer | Tracks retry attempts for failed operations |
| `InitializeProcessing` | Boolean | Indicates if this is an initialization message |
| `GroupNumber` | Integer | Identifies the processing group number |
| `ServiceProviderId` | Integer | Service provider identifier for processing |
| `IntegrationType` | Integer | Type of integration (ThingSpace = specific enum value) |

### Low-Level Flow:
1. **External Scheduler/Trigger** → Publishes initial SQS message to `ThingSpaceDeviceUsageQueueURL`
2. **Lambda Function** → Receives SQS event via `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
3. **Message Processing** → Extracts attributes using `GetMessageQueueValues()`
4. **Continuation Messages** → Lambda publishes follow-up messages to continue processing

---

## 2. Batch Size Configuration

Multiple batch size configurations exist depending on the operation type:

### 2.1 ThingSpace Device Usage Processing Batch Size

```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:38
private int BatchSize = Convert.ToInt32(Environment.GetEnvironmentVariable("BatchSize"));
```

**Simplified Process:**
1. Read BatchSize from environment variables → Convert to integer → Use for device processing limits

**Usage:** Controls the number of devices processed per API call cycle in usage retrieval operations.

### 2.2 SQL Bulk Copy Operations Batch Size

```csharp
// AwsFunctionBase.cs:305
bulkCopy.BatchSize = Amop.Core.Constants.SQLConstant.BatchSize;
```

**Simplified Process:**
1. Set SQL bulk copy batch size → Controls how many records are inserted at once → Improves database performance

**Location:** Line 305 in `AwsFunctionBase.cs`  
**Usage:** Controls SQL bulk copy operations for ThingSpace device staging

### 2.3 Jasper Device Batching Operations

```csharp
// AwsFunctionBase.cs:198-201
command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
command.Parameters.AddWithValue("@BatchSize", batchSize);
```

**Simplified Process:**
1. Add service provider ID parameter → Add batch size parameter → Execute stored procedure with batching

**Usage:** Controls Jasper device processing batches via stored procedure `usp_GetBatchedJasperSimCardCountByServiceProviderId`

### Low-Level Batch Processing Flow:

```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:176-202
string cmdText = $"SELECT TOP {BatchSize} [ICCID], [ServiceProviderId], [BillingCycleEndDate] FROM [dbo].[ThingSpaceDeviceUsageICCICDsToProcess] WHERE ServiceProviderId = @ServiceProviderId AND GroupNumber = {sqsValues.GroupNumber}";

// Processing loop with batch size control
for (int iRow = 0; iRow < iccids.Count; iRow++)
{
    if (context.Context.RemainingTime.TotalSeconds > 60)
    {
        // Process individual device
        deviceUsage = await client.GetThingSpaceUsageAsync(iccids[iRow], sessionToken.sessionToken, accessToken.Access_Token, ThingSpaceDeviceUsageGetURL, context.logger);
    }
}
```

**Simplified Process:**
1. Build SQL query with TOP BatchSize → Execute query → Loop through results → Check remaining time → Process each device individually

---

## 3. ThingSpace API Integration

### 3.1 GetThingSpaceDeviceAsync - API Implementation

```csharp
// ThingSpaceCommon.cs:61-91
public static async Task<DeviceResponse> GetThingSpaceDeviceAsync(string iccid, string baseUrl, ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, IKeysysLogger logger)
{
    logger.LogInfo("INFO", "GetThingSpaceDeviceAsync");
    logger.LogInfo("INFO", $"iccid: {iccid}");

    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls13 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

    using (var client = new HttpClient(new LambdaLoggingHandler()))
    {
        client.BaseAddress = new Uri($"{baseUrl.TrimEnd('/')}/api/m2m/v1/devices/actions/list");
        logger.LogInfo("Endpoint", client.BaseAddress);
        client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
        client.DefaultRequestHeaders.Add("Accept", "application/json");
        client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);
        
        var jsonDeviceContent = $"{{\"deviceId\":{{\"id\":\"{iccid}\",\"kind\":\"iccid\"}}}}";
        var contDevice = new StringContent(jsonDeviceContent, Encoding.UTF8, "application/json");
        var response = await client.PostAsync(client.BaseAddress, contDevice);
        
        if (response.IsSuccessStatusCode)
        {
            string responseBody = await response.Content.ReadAsStringAsync();
            var body = JsonConvert.DeserializeObject<ThingSpaceDeviceResponseRootObject>(responseBody);
            return body?.devices?.FirstOrDefault();
        }
        else
        {
            var responseBody = await response.Content.ReadAsStringAsync();
            logger.LogInfo("EXCEPTION", responseBody);
            return null;
        }
    }
}
```

**Simplified Process:**
1. Log start of API call → Set TLS security protocols → Create HTTP client → Set base URL and headers → Create JSON payload → Send POST request → Check response success → Parse JSON response → Return device data or null

### 3.2 ThingSpace Carrier Configuration

**Supported Carriers:**
- Verizon ThingSpace IoT
- Verizon ThingSpace PN

**Base Address:** `https://thingspace.verizon.com/`  
**Endpoint:** `/api/m2m/v1/devices/actions/list`

### 3.3 Authentication Flow

#### Access Token Generation
```csharp
// ThingSpaceCommon.cs:33-58
public static ThingSpaceTokenResponse GetAccessToken(ThingSpaceAuthentication thingSpaceAuth)
{
    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls13 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;
    var base64Service = new Base64Service();
    using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
    {
        client.BaseAddress = new Uri(thingSpaceAuth.BaseUrl);
        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        string encodedThing = base64Service.Base64Encode(thingSpaceAuth.ClientId + ":" + thingSpaceAuth.ClientSecret);
        client.DefaultRequestHeaders.Add("Authorization", "Basic " + encodedThing);
        client.DefaultRequestHeaders.Add("Accept", "application/json");

        var formContent = new Dictionary<string, string>();
        formContent.Add("grant_type", "client_credentials");
        var content = new FormUrlEncodedContent(formContent);

        var responseToken = client.PostAsync(thingSpaceAuth.AuthTokenUrl, content);
        responseToken.Wait();
        if (responseToken.Result.IsSuccessStatusCode)
        {
            var result = responseToken.Result.Content.ReadAsStringAsync().Result;
            return JsonConvert.DeserializeObject<ThingSpaceTokenResponse>(result);
        }

        return null;
    }
}
```

**Simplified Process:**
1. Set TLS protocols → Create Base64 service → Create HTTP client → Encode client credentials to Base64 → Add Basic authorization header → Create form content with grant_type → Send POST request → Check success → Parse JSON response → Return token or null

**POST** `https://thingspace.verizon.com/api/ts/v1/oauth2/token`

**Response:**
```json
{
  "access_token": "eyJraWQiOiJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.xxxxx.yyyyy",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

#### Session Token Generation
```csharp
// ThingSpaceCommon.cs:134-158
public static ThingSpaceLoginResponse GetSessionToken(ThingSpaceAuthentication thingSpaceAuth, ThingSpaceTokenResponse accessToken)
{
    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls13 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;
    var base64Service = new Base64Service();
    using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
    {
        client.BaseAddress = new Uri(thingSpaceAuth.BaseUrl);
        client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
        client.DefaultRequestHeaders.Add("Accept", "application/json");
        string password = base64Service.Base64Decode(thingSpaceAuth.Password);

        string jsonContent = "{\"username\":\"" + thingSpaceAuth.Username + "\",\"password\":\"" + password + "\"}";
        var cont = new StringContent(jsonContent, Encoding.UTF8, "application/json");

        var responseLogin = client.PostAsync(thingSpaceAuth.AuthUrl, cont);
        responseLogin.Wait();
        if (responseLogin.Result.IsSuccessStatusCode)
        {
            var result = responseLogin.Result.Content.ReadAsStringAsync().Result;
            return JsonConvert.DeserializeObject<ThingSpaceLoginResponse>(result);
        }

        return null;
    }
}
```

**Simplified Process:**
1. Set TLS protocols → Create Base64 service → Create HTTP client → Add Bearer token authorization → Decode password from Base64 → Create JSON login payload → Send POST request → Check success → Parse JSON response → Return session token or null

**POST** `https://thingspace.verizon.com/api/m2m/v1/session/login`

**Response:**
```json
{
  "sessionToken": "vzm2m-6d4aa34c-fd34-42b1-8e0e-9ab123456789",
  "expiresIn": 1800
}
```

### 3.4 Request Headers Configuration

```csharp
// Standard headers for ThingSpace API calls
client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
client.DefaultRequestHeaders.Add("Accept", "application/json");
client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);
```

**Simplified Process:**
1. Add Bearer token for authorization → Add JSON accept header → Add ThingSpace session token header

### 3.5 Request Payload Options

**With Account Name and Largest Device ID:**
```json
{
  "accountName": "0242390116-00001",
  "largestDeviceIdSeen": "<Integer Value>"
}
```

**Single Device Query:**
```json
{
  "deviceId": {
    "id": "{iccid}",
    "kind": "iccid"
  }
}
```

### Low-Level API Integration Flow:
1. **Authentication Setup** → Retrieve stored credentials from database
2. **Access Token Request** → POST to `/api/ts/v1/oauth2/token` with Basic auth
3. **Session Token Request** → POST to `/api/m2m/v1/session/login` with Bearer token
4. **API Calls** → Use both tokens in subsequent device/usage requests
5. **Error Handling** → Log failures and return null on unsuccessful responses

---

## 4. Delayed Message Handling

**Answer:** Delayed messages are handled using **explicit SQS DelaySeconds mechanism**, not CloudWatch visibility timeout.

### Evidence from Code:

All SQS messages sent by the `AltaworxThingSpaceAWSGetDeviceUsage` Lambda function use explicit delays:

#### 5-Second Delay (Standard Processing):
```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:471-497
var request = new SendMessageRequest
{
    DelaySeconds = delaySeconds, // Default: 5 seconds
    MessageAttributes = new Dictionary<string, MessageAttributeValue>
    {
        {
            "InitializeProcessing", new MessageAttributeValue
            {
                DataType = "String", StringValue = initializeProcessing.ToString()
            }
        },
        {
            "GroupNumber", new MessageAttributeValue
            {
                DataType = "String", StringValue = groupNumber.ToString()
            }
        },
        {
            "ServiceProviderId", new MessageAttributeValue
            {
                DataType = "String", StringValue = serviceProviderId.ToString()
            }
        }
    },
    MessageBody = requestMsgBody,
    QueueUrl = ThingSpaceDeviceUsageQueueURL
};
```

**Simplified Process:**
1. Create message request with delay → Add message attributes (processing flag, group number, service provider) → Set message body and queue URL → Send to SQS

#### 5-Minute Delay (Notification Messages):
```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:520-540
var request = new SendMessageRequest
{
    DelaySeconds = (int)TimeSpan.FromMinutes(5).TotalSeconds, // 300 seconds
    MessageAttributes = new Dictionary<string, MessageAttributeValue>
    {
        {
            "RetryCount", new MessageAttributeValue
            { DataType = "String", StringValue = "0"}
        },
        {
            "IntegrationType", new MessageAttributeValue
            { DataType = "String", StringValue = ((int)IntegrationType.ThingSpace).ToString() }
        },
        {
            "ServiceProviderId", new MessageAttributeValue
            { DataType = "String", StringValue = serviceProviderId.ToString() }
        }
    },
    MessageBody = requestMsgBody,
    QueueUrl = ThingSpaceDeviceNotificationQueueURL
};
```

**Simplified Process:**
1. Calculate 5-minute delay in seconds → Create message with retry count, integration type, and service provider → Send to notification queue with delay

#### 30-Second Delay (Group Processing):
```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:446-452
private void SendProcessMessagesToQueue(KeySysLambdaContext context, int groupCount, int serviceProviderId)
{
    for (int iGroup = 0; iGroup <= groupCount; iGroup++)
    {
        SendProcessMessageToQueue(context, iGroup, serviceProviderId, 30); // 30-second delay
    }
}
```

**Simplified Process:**
1. Loop through all groups → Send message for each group with 30-second delay → Allows parallel processing of different groups

### Low-Level Delayed Message Flow:
1. **Message Creation** → Construct `SendMessageRequest` with specific `DelaySeconds`
2. **SQS Submission** → Message held in SQS queue for specified delay period
3. **Delayed Delivery** → SQS automatically delivers message after delay expires
4. **Lambda Invocation** → Delayed message triggers Lambda function execution

---

## 5. Failed ICCID Logging

**Answer:** Failed ICCIDs are logged through the `LogInfo()` method in multiple scenarios within the `AltaworxThingSpaceAWSGetDeviceUsage` Lambda function.

### 5.1 Logging Locations

#### API Call Failures:
```csharp
// ThingSpaceCommon.cs:84-88
else
{
    var responseBody = await response.Content.ReadAsStringAsync();
    logger.LogInfo("EXCEPTION", responseBody);
    return null;
}
```

**Simplified Process:**
1. API call fails → Read error response body → Log exception with response details → Return null to indicate failure

#### Device Usage API Failures:
```csharp
// ThingSpaceDeviceDetailService.cs:130-136
else
{
    _logger.LogInfo("ThingSpaceGetDeviceUsage", $"ThingSpaceGetDeviceUsage:Call to {thingSpaceDeviceUsageGetURL} with iccid: {iccidToProcess.iccid} failed.");
}
```

**Simplified Process:**
1. Device usage API call fails → Log specific error message with ICCID and URL → Continue processing other devices

#### Exception Handling with ICCID Context:
```csharp
// ThingSpaceDeviceDetailService.cs:133-136
catch (Exception ex)
{
    _logger.LogInfo("EXCEPTION", $"Error Getting Device Usage for {iccidToProcess.iccid}: {JsonConvert.SerializeObject(ex)}");
}
```

**Simplified Process:**
1. Exception occurs during processing → Serialize exception to JSON → Log with specific ICCID context → Continue to next device

#### General Processing Exceptions:
```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:245-248
catch (Exception ex)
{
    LogInfo(context, "EXCEPTION", $"Error Getting Device Usage for {iccids[iRow].iccid}: {JsonConvert.SerializeObject(ex)}");
}
```

**Simplified Process:**
1. Exception during device processing → Get current ICCID from array → Serialize exception → Log with ICCID identifier → Continue processing

### 5.2 Log Destinations
- **AWS CloudWatch Logs** (via `context.Logger.LogLine()`)
- **Custom logging infrastructure** (via `LogInfo()` method)

### Low-Level Failed ICCID Logging Flow:
1. **API Call Execution** → Attempt ThingSpace API call for specific ICCID
2. **Failure Detection** → Check `response.IsSuccessStatusCode` or catch exceptions
3. **Context Logging** → Log failure with ICCID identifier and error details
4. **CloudWatch Integration** → Logs automatically forwarded to CloudWatch Logs
5. **Continuation** → Processing continues with next ICCID in batch

---

## 6. Retry Configuration

Multiple retry configurations exist for different scenarios in the ThingSpace implementation:

### 6.1 HTTP Retry Policy Configuration

```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:400-404
private IAsyncPolicy GetHttpRetryPolicy(KeySysLambdaContext context)
{
    var policyFactory = new PolicyFactory(context.logger);
    return policyFactory.GetHttpRetryPolicy(3); // 3 retries
}
```

**Simplified Process:**
1. Create policy factory with logger → Get HTTP retry policy with 3 attempts → Return configured policy for use in API calls

### 6.2 SQL Bulk Copy Operations

```csharp
// AwsFunctionBase.cs:301-315
using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
{
    bulkCopy.DestinationTableName = tableName;
    bulkCopy.BulkCopyTimeout = Amop.Core.Constants.SQLConstant.TimeoutSeconds;
    bulkCopy.BatchSize = Amop.Core.Constants.SQLConstant.BatchSize;
    
    if (columnMappings != null && columnMappings.Count > 0)
    {
        foreach (var mapping in columnMappings)
        {
            bulkCopy.ColumnMappings.Add(mapping);
        }
    }
    bulkCopy.WriteToServer(table);
}
```

**Simplified Process:**
1. Create bulk copy object → Set destination table and timeout → Set batch size → Add column mappings if provided → Execute bulk copy to server

### 6.3 Exception Handling with Retry Logic

```csharp
// AwsFunctionBase.cs:318-329
catch (SqlException ex)
{
    LogInfo(context, LogTypeConstant.Exception, $"Exception when executing SQL command: {ex.Message}, ErrorCode: {ex.ErrorCode}-{ex.Number}");
}
catch (InvalidOperationException ex)
{
    LogInfo(context, LogTypeConstant.Exception, $"Exception when connecting to database: {ex.Message}");
}
catch (Exception ex)
{
    LogInfo(context, LogTypeConstant.Exception, $"Exception when Bulk Copying Table: {tableName}, {ex.Message}");
}
```

**Simplified Process:**
1. Catch SQL-specific exceptions → Log with error code and number → Catch connection exceptions → Log connection issues → Catch general exceptions → Log with table name context

### Low-Level Retry Flow:
1. **Policy Factory Creation** → Initialize `PolicyFactory` with logger context
2. **HTTP Retry Policy Setup** → Configure policy for 3 retry attempts
3. **API Call Execution** → Execute HTTP request within retry policy wrapper
4. **Failure Detection** → Policy automatically retries on transient failures
5. **Exponential Backoff** → Increasing delays between retry attempts
6. **Final Failure** → After max retries, log exception and continue processing

---

## 7. Partially Unprocessed Groups

**Answer:** If a group remains partially unprocessed after maximum retries in the `AltaworxThingSpaceAWSGetDeviceUsage` Lambda function, **the process continues to the next group without failing the entire batch**.

### 7.1 Processing Flow Analysis

#### Individual Device Processing with Error Isolation:
```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:238-248
try
{
    deviceUsage = await client.GetThingSpaceUsageAsync(iccids[iRow], sessionToken.sessionToken, accessToken.Access_Token, ThingSpaceDeviceUsageGetURL, context.logger);
    // Get device daily usage
    string usageDateStr = usageDate.ToString("s", System.Globalization.DateTimeFormatInfo.InvariantInfo) + "Z";
    deviceDailyUsage = await client.GetThingSpaceDailyUsageAsync(iccids[iRow], sessionToken.sessionToken, accessToken.Access_Token, ThingSpaceDeviceUsageGetURL, usageDateStr, usageDateStr, context.logger);
}
catch (Exception ex)
{
    LogInfo(context, "EXCEPTION", $"Error Getting Device Usage for {iccids[iRow].iccid}: {JsonConvert.SerializeObject(ex)}");
    // Processing continues to next device
}
```

**Simplified Process:**
1. Try to get device usage → Try to get daily usage → If any exception occurs → Log error with ICCID → Continue to next device without stopping

#### Successful Device Cleanup:
```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:307-308
sqsValues.DevicesProcessed.Add(iccids[iRow].iccid);
RemoveProcessedICCID(context, iccids[iRow].iccid);
```

**Simplified Process:**
1. Add ICCID to processed list → Remove ICCID from processing queue → Device won't be processed again

#### Group Continuation Logic:
```csharp
// AltaworxThingSpaceAWSGetDeviceUsage.cs:322
SendProcessMessageToQueue(context, sqsValues.GroupNumber, sqsValues.ServiceProviderId);
```

**Simplified Process:**
1. Send message to continue processing same group → Failed devices remain in queue → Will be retried in next execution

### 7.2 Consequences of Partial Processing

#### Positive Aspects:
- **Fault Tolerance:** Individual device failures don't stop group processing
- **Data Preservation:** Successfully processed devices are saved and removed from queue
- **Continuation:** Failed devices remain in processing queue for future attempts
- **Logging:** All failures are logged with specific ICCID context

#### Potential Issues:
- **Infinite Loops:** Failed devices may be repeatedly attempted without upper limit
- **Resource Consumption:** Persistent failures consume processing cycles
- **Data Inconsistency:** Groups may have partial completion status

### Low-Level Partial Processing Flow:
1. **Group Processing Start** → Begin processing devices in current group
2. **Individual Device Processing** → Process each ICCID with isolated error handling
3. **Success Path** → Save data, log success, remove from processing queue
4. **Failure Path** → Log error, continue to next device without stopping group
5. **Group Completion** → Send continuation message for same group number
6. **Next Iteration** → Failed devices remain in queue for retry in next cycle

### Recovery Mechanism:
The system relies on **eventual consistency** where failed devices will be retried in subsequent Lambda invocations until they either succeed or are manually removed from the processing queue.

---

## Environment Variables

The `AltaworxThingSpaceAWSGetDeviceUsage` Lambda function uses the following environment variables:

| Variable | Purpose | Code Location |
|----------|---------|---------------|
| `ThingSpaceDeviceUsageGetURL` | API endpoint for device usage retrieval | Line 35 |
| `ThingSpaceDeviceNotificationQueueURL` | SQS queue for notification messages | Line 36 |
| `ThingSpaceDeviceUsageQueueURL` | SQS queue for continuation messages | Line 37 |
| `BatchSize` | Number of devices processed per batch | Line 38 |

## Key Constants

| Constant | Value | Purpose | Location |
|----------|-------|---------|----------|
| `DEFAULT_BILLING_CYCLE_END_DAY` | 23 | Default billing cycle end day | Line 31, ThingSpaceCommon.cs |
| `DEFAULT_BILLING_CYCLE_END_HOUR` | 0 | Default billing cycle end hour | Line 32, ThingSpaceCommon.cs |
| `SQL_TRANSIENT_RETRY_MAX_COUNT` | 3 | Maximum SQL retry attempts | Referenced in RetryPolicyHelper |
| `HTTP_RETRY_COUNT` | 3 | HTTP retry count | Referenced in PolicyFactory |

---

## Summary

This comprehensive analysis reveals that the ThingSpace AWS Lambda implementation follows a **resilient, batch-oriented processing pattern** with:

- **External trigger initiation** for SQS message publishing with comprehensive message attributes
- **Flexible batch size configuration** across different operation types  
- **Robust authentication flow** with dual-token system (Access + Session)
- **Explicit delay mechanisms** using SQS DelaySeconds (5s, 30s, 5min)
- **Comprehensive error logging** with ICCID-specific context
- **Multi-layered retry policies** for HTTP and SQL operations
- **Fault-tolerant group processing** with individual device error isolation

The system prioritizes **data consistency** and **processing continuity** over strict transactional boundaries, making it suitable for large-scale IoT device management scenarios where occasional individual failures are acceptable within the broader processing workflow.