# ThingSpace Carrier Implementation Analysis Report

## Overview
This document provides a comprehensive analysis of the ThingSpace carrier implementation based on the examination of the AWS Lambda functions and supporting classes in the AltaworxThingSpaceAWSGetDevices repository.

---

## 1. Who publishes the first SQS message to ThingSpaceDeviceQueueURL?

**Answer:** The first SQS message to `ThingSpaceDeviceQueueURL` is published by **external triggers or schedulers** outside of this codebase.

**Evidence:**
- The `Function.FunctionHandler()` method in `AltaworxThingSpaceAWSGetDevices.cs` **receives** SQS messages but does not publish the initial message
- The function processes incoming SQS events: `public void FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
- Environment variable `ThingSpaceDestinationQueueGetDevicesURL` is used for **continuation messages**, not the initial trigger
- The function can run without SQS events (lines 74-80), suggesting it can be triggered directly or by external schedulers

**Code References:**
```csharp
// Lines 59-73: Processes incoming SQS messages
if (sqsEvent != null && sqsEvent.Records != null)
{
    processedRecordCount = sqsEvent.Records.Count;
    // Process each record
}
else
{
    // Can run without SQS event
    var sqsValues = new GetDeviceSqsValues();
    processedRecordCount = 1;
    TryProcessDeviceList(keysysContext, sqsValues);
}
```

---

## 2. What is the configured batch size limit for device groups?

**Answer:** The batch size is configured through **`Amop.Core.Constants.SQLConstant.BatchSize`** for SQL operations.

**Evidence:**
- In `AwsFunctionBase.cs` line 305: `bulkCopy.BatchSize = Amop.Core.Constants.SQLConstant.BatchSize;`
- For Jasper device batching, the batch size is passed as a parameter to stored procedure `usp_GetBatchedJasperSimCardCountByServiceProviderId`
- The exact value is defined in the `Amop.Core.Constants.SQLConstant` class (not visible in this repository)

**Code References:**
```csharp
// Line 305 in AwsFunctionBase.cs
bulkCopy.BatchSize = Amop.Core.Constants.SQLConstant.BatchSize;

// Lines 198-201 in AwsFunctionBase.cs
command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
command.Parameters.AddWithValue("@BatchSize", batchSize);
```

---

## 3. For GetThingSpaceDeviceAsync, provide the exact API endpoint and rate limits.

**Answer:** 
- **Exact API Endpoint:** `{baseUrl}/api/m2m/v1/devices/actions/list`
- **Rate Limits:** Handles HTTP 429 (Too Many Requests) with dynamic retry delays parsed from response

**Evidence:**
- In `ThingSpaceCommon.cs` line 70: `client.BaseAddress = new Uri($"{baseUrl.TrimEnd('/')}/api/m2m/v1/devices/actions/list");`
- Rate limiting is handled by Polly retry policies in `RetryPolicyHelper.cs`
- HTTP 429 responses trigger dynamic wait times based on the API response content

**Code References:**
```csharp
// Line 70 in ThingSpaceCommon.cs
client.BaseAddress = new Uri($"{baseUrl.TrimEnd('/')}/api/m2m/v1/devices/actions/list");

// Lines 159-178 in RetryPolicyHelper.cs - Rate limit handling
if (retryResponse.Result != null && (int)retryResponse.Result.StatusCode == CommonConstants.TOO_MANY_REQUEST_HTTP_STATUS_CODE)
{
    // Parse the response for the seconds
    var responseBody = retryResponse.Result.Content.ReadAsStringAsync().Result;
    var revIOError = JsonConvert.DeserializeObject<RevIoErrorResponse>(responseBody);
    // Extract wait time from response
}
```

**Request Format:**
```json
{
  "deviceId": {
    "id": "{iccid}",
    "kind": "iccid"
  }
}
```

---

## 4. How are delayed messages (30s, 5min) handled—CloudWatch visibility or explicit delay?

**Answer:** Delayed messages are handled using **explicit SQS DelaySeconds** mechanism, not CloudWatch visibility timeout.

**Evidence:**
- All SQS messages are sent with `DelaySeconds = (int)TimeSpan.FromSeconds(5).TotalSeconds` (5-second delay)
- No CloudWatch visibility timeout configuration found in the codebase
- The delay is explicitly set in the `SendMessageRequest` object

**Code References:**
```csharp
// Lines 539 & 596 in AltaworxThingSpaceAWSGetDevices.cs
DelaySeconds = (int)TimeSpan.FromSeconds(5).TotalSeconds,

// SendMessageRequest configuration
var request = new SendMessageRequest
{
    DelaySeconds = (int)TimeSpan.FromSeconds(5).TotalSeconds,
    MessageAttributes = new Dictionary<string, MessageAttributeValue>
    {
        // Message attributes
    },
    MessageBody = requestMsgBody,
    QueueUrl = thingSpaceDestinationQueueGetDevicesURL
};
```

**Note:** The 5-second delay is hardcoded. No 30-second or 5-minute delays were found in the current codebase.

---

## 5. Where are failed ICCIDs logged if device fetch fails?

**Answer:** Failed ICCIDs are logged through the **`LogInfo()` method** in multiple scenarios:

**Logging Locations:**
1. **Bad API Data:** Line 222 - `LogInfo(context, "WARN", "Bad data from API: ICCID/IMEI/MSISDN cannot empty!");`
2. **API Call Failures:** Lines 394-397 - Logs full error details when ThingSpace API calls fail
3. **Authentication Failures:** Lines 404-421 - Logs when access tokens or session tokens fail
4. **General Exceptions:** Lines 86, 124 - Exception handling with full context

**Code References:**
```csharp
// Line 222 - Bad ICCID data
LogInfo(context, "WARN", "Bad data from API: ICCID/IMEI/MSISDN cannot empty!");

// Lines 394-397 - API failure logging
var errorMessage = $"ThingSpaceGetDevices:Call to {ThingSpaceDevicesGetURL} with AccountNumber failed. {responseAccounDevice.Result.StatusCode} {errorBodyResult}";
LogInfo(context, "ThingSpaceGetDevices", errorMessage);

// Lines 86-87 - General exception logging
catch (Exception ex)
{
    context.Logger.LogLine($"EXCEPTION : {ex.Message}");
}
```

**Log Destinations:**
- AWS CloudWatch Logs (via `context.Logger.LogLine()`)
- Custom logging infrastructure (via `LogInfo()` method)

---

## 6. Provide retry configuration for Polly SQL/HTTP retries.

**Answer:** Multiple retry configurations exist for different scenarios:

### SQL Transient Retries:
- **Max Retry Count:** `SQL_TRANSIENT_RETRY_MAX_COUNT = 3`
- **Policy:** `GetSqlTransientPolicy()` with fallback handling
- **Location:** `RetryPolicyHelper.cs` lines 23-30

### HTTP Retries:
- **Default Retry Count:** `CommonConstants.NUMBER_OF_RETRIES`
- **Retry Delay Formula:** `TimeSpan.FromSeconds(Math.Pow(CommonConstants.API_ERROR_DELAY_IN_SECONDS, retryAttempt))`
- **Exponential Backoff:** Yes, using power calculation

### ThingSpace-Specific Retries:
- **Max Retry Count:** `MAX_RETRY_COUNT = 3` (line 35 in AltaworxThingSpaceAWSGetDevices.cs)
- **Retry Scenarios:**
  - EXCEPTION messages
  - EXPIRED token messages (60-second threshold)

**Code References:**
```csharp
// SQL retry configuration
public const int SQL_TRANSIENT_RETRY_MAX_COUNT = 3;

// HTTP retry delay calculation
public static TimeSpan CalculateRetryDelay(int retryAttempt)
{
    return TimeSpan.FromSeconds(Math.Pow(CommonConstants.API_ERROR_DELAY_IN_SECONDS, retryAttempt));
}

// ThingSpace retry logic (lines 159-182)
if (sqsValues.RetryCount < MAX_RETRY_COUNT)
{
    sqsValues.RetryCount++;
    LogInfo(context, "Requeue Get Values", $"Requeueing with retry count {sqsValues.RetryCount}");
    SendMessageToQueue(context, sqsValues, ThingSpaceDestinationQueueGetDevicesURL);
    return;
}
```

---

## 7. What happens if a group remains partially unprocessed after retries?

**Answer:** If a group remains partially unprocessed after maximum retries, the **process fails and throws an exception**.

**Processing Flow:**
1. **Retry Logic:** Up to `MAX_RETRY_COUNT = 3` attempts
2. **Failure Handling:** After max retries, logs failure and throws exception
3. **No Partial Recovery:** No mechanism to process remaining items in a partially failed group
4. **Complete Failure:** The entire processing cycle is considered failed

**Code References:**
```csharp
// Lines 166-170 in AltaworxThingSpaceAWSGetDevices.cs
else
{
    LogInfo(context, "Max retryCount", $"Reached maximum retry count of {MAX_RETRY_COUNT}, considered as failed");
    throw e;
}

// Lines 179-182 - Unexpected exceptions
else
{
    LogInfo(context, "Unexpected Exception", $"Considered as critical, no retry executed");
    throw e;
}
```

**Consequences:**
- **Complete Cycle Failure:** The entire device processing cycle fails
- **No Partial Success:** No mechanism to save successfully processed devices from a partially failed batch
- **Manual Intervention Required:** Failed groups require manual investigation and reprocessing
- **Data Loss Risk:** Successfully processed devices in the failed batch may need to be reprocessed

---

## Environment Variables

The following environment variables are used:

| Variable | Purpose | Location |
|----------|---------|----------|
| `ThingSpaceDevicesGetURL` | API endpoint for device retrieval | Line 31 |
| `MaxCyclesToProcess` | Maximum processing cycles limit | Line 32 |
| `ThingSpaceDestinationQueueGetDevicesURL` | SQS queue for continuation messages | Line 33 |
| `DeviceUsageQueueURL` | SQS queue for device usage processing | Line 34 |

---

## Key Constants

| Constant | Value | Purpose |
|----------|--------|---------|
| `MAX_RETRY_COUNT` | 3 | Maximum retry attempts for ThingSpace operations |
| `EXPIRED_TIME` | 60 | Token expiration threshold in seconds |
| `SQL_TRANSIENT_RETRY_MAX_COUNT` | 3 | Maximum SQL retry attempts |
| `DEFAULT_BILLING_CYCLE_END_DAY` | 23 | Default billing cycle end day |
| `DEFAULT_BILLING_CYCLE_END_HOUR` | 0 | Default billing cycle end hour |

---

## Summary

The ThingSpace implementation uses a robust retry mechanism with exponential backoff for HTTP requests and fixed retry counts for critical operations. The system handles partial failures by completely failing the batch rather than attempting partial recovery, which ensures data consistency but may require manual intervention for failed groups. All logging is centralized through the `LogInfo()` method with output to AWS CloudWatch Logs.