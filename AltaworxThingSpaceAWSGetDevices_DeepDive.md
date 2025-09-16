## AltaworxThingSpaceAWSGetDevices – Deep Dive, Q&A Mapping, and SQL Flow

### Scope
- Focus: `AltaworxThingSpaceAWSGetDevices` (ThingSpace carrier).
- Maps each question to concrete code references and behavior.
- Includes how stored procedures integrate into the flow.

---

### 1) What triggers this Lambda—another Lambda, schedule, or manual?
- Trigger: SQS (`SQSEvent`). Also supports manual invocation when `sqsEvent` is null.

```43:70:/workspace/AltaworxThingSpaceAWSGetDevices.cs
public void FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
{
    KeySysLambdaContext keysysContext = null;
    try
    {
        keysysContext = BaseFunctionHandler(context);
        ...
        if (sqsEvent != null && sqsEvent.Records != null)
        {
            processedRecordCount = sqsEvent.Records.Count;
            LogInfo(keysysContext, "STATUS", $"ThingSpaceGetDevices::Beginning to process {processedRecordCount} records...");
            foreach (var record in sqsEvent.Records)
            {
                LogInfo(keysysContext, "MessageId", record.MessageId);
                LogInfo(keysysContext, "EventSource", record.EventSource);
                LogInfo(keysysContext, "Body", record.Body);
                var sqsValues = getMessageQueueValues(keysysContext, record);
                TryProcessDeviceList(keysysContext, sqsValues);
            }
        }
```

```74:80:/workspace/AltaworxThingSpaceAWSGetDevices.cs
else
{
    var sqsValues = new GetDeviceSqsValues();
    processedRecordCount = 1;
    TryProcessDeviceList(keysysContext, sqsValues);
}
```

---

### 2) Why is SQL retry done first, and what issue does it prevent?
- First provider selection triggers truncation of staging with a SQL transient retry to ensure a clean start and handle transient DB faults.

```96:114:/workspace/AltaworxThingSpaceAWSGetDevices.cs
if (sqsValues.CurrentServiceProviderId == 0)
{
    var serviceProvider = ServiceProviderCommon.GetNextServiceProviderId(...);
    switch (serviceProvider)
    {
        ...
        default:
            TruncateThingSpaceDeviceAndUsageStagingWithPolicy(context.CentralDbConnectionString, context.logger);
            sqsValues.CurrentServiceProviderId = serviceProvider;
            break;
    }
}
```

```451:457:/workspace/AltaworxThingSpaceAWSGetDevices.cs
private void TruncateThingSpaceDeviceAndUsageStagingWithPolicy(string connectionString, IKeysysLogger logger)
{
    logger.LogInfo("SUB", $"TruncateThingSpaceDeviceAndUsageStagingWithPolicy()");
    var errorMessages = new List<string>();
    var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages);
    sqlTransientRetryPolicy.Execute(() => TruncateThingSpaceDeviceAndUsageStaging(connectionString, logger));
}
```

---

### 3) Are device and BAN staging tables cleared at the start?
- Yes for ThingSpace device and usage staging; BAN staging is not part of this Lambda.

```459:471:/workspace/AltaworxThingSpaceAWSGetDevices.cs
private void TruncateThingSpaceDeviceAndUsageStaging(string connectionString, IKeysysLogger logger)
{
    using (var connection = new SqlConnection(connectionString))
    using (var command = new SqlCommand("usp_ThingSpace_Truncate_DeviceAndUsageStaging", connection))
    {
        command.CommandType = CommandType.StoredProcedure;
        connection.Open();
        command.ExecuteNonQuery();
    }
}
```

Your SQL:
```sql
CREATE PROCEDURE [dbo].[usp_ThingSpace_Truncate_DeviceAndUsageStaging]
AS
BEGIN
 truncate table [dbo].[ThingSpaceDeviceStaging]
 truncate table [dbo].[ThingSpaceDeviceUsageStaging]
 truncate table [dbo].[ThingSpaceDeviceUsageICCICDsToProcess]
END
```

---

### 4) Aren’t staging tables already cleared after the previous run?
- The Lambda guarantees cleanliness each new provider-run by truncating at start, even if previous runs ended early.

---

### 5) Which staging table stores BAN, FAN, and Number statuses?
- Not applicable here. This Lambda stages into `ThingSpaceDeviceStaging`.

```225:233:/workspace/AltaworxThingSpaceAWSGetDevices.cs
LogInfo(context, "STATUS", "SQL Bulk Copy Start");
SqlBulkCopy(context, context.CentralDbConnectionString, table, "ThingSpaceDeviceStaging");
```

---

### 6) Are BAN list statuses read from BillingAccountNumberStatusStaging or elsewhere?
- Not referenced in this Lambda.

---

### 7) Which Telegence API endpoint is called in GetTelegenceDevicesAsync?
- Not applicable. For ThingSpace, single-device query uses:

```70:77:/workspace/ThingSpaceCommon.cs
client.BaseAddress = new Uri($"{baseUrl.TrimEnd('/')}/api/m2m/v1/devices/actions/list");
var response = await client.PostAsync(client.BaseAddress, contDevice);
```

---

### 8) What is the page size/limit for Telegence API calls?
- Not applicable. ThingSpace pagination is controlled by `hasMoreData` and `largestDeviceIdSeen`.

---

### 9) How does the system know all API pages are processed?
- Uses `deviceList.hasMoreData` and captures `DeviceId` from the last record’s `extendedAttributes` to set `LargestDeviceIdSeen`.

```358:387:/workspace/AltaworxThingSpaceAWSGetDevices.cs
int deviceTotal = deviceList != null && deviceList.devices != null ? deviceList.devices.Count : 0;
sqsValues.HasMoreData = deviceTotal != 0;
isLastCycle = !sqsValues.HasMoreData;
if (deviceList?.devices != null)
{
    for (int i = 0; i < deviceTotal; i++)
    {
        sqsValues.ThingSpaceDeviceList.Add(deviceList.devices[i]);
        if (i == deviceTotal - 1)
        {
            sqsValues.HasMoreData = deviceList.hasMoreData.ToString().ToLower() == "true";
            if (sqsValues.HasMoreData)
            {
                var deviceIdAttrib = deviceList.devices[i].extendedAttributes.FirstOrDefault(x => x.key == "DeviceId");
                if (deviceIdAttrib != null)
                {
                    sqsValues.LargestDeviceIdSeen = deviceIdAttrib.value;
                }
                else
                {
                    LogInfo(context, "ThingSpaceGetDevices", "ThingSpaceGetDevices: DeviceId attrib not found.");
                }
            }
            isLastCycle = !sqsValues.HasMoreData;
        }
    }
}
```

---

### 10) What parameters are used in GetTelegenceDeviceBySubscriberNumber?
- Not applicable in this Lambda.

---

### 11) What happens to devices that fail validation?
- Devices lacking identifiers are skipped and not staged; a warning is logged.

```215:224:/workspace/AltaworxThingSpaceAWSGetDevices.cs
if (thingSpaceDevice.deviceIds != null && thingSpaceDevice.deviceIds.Count > 0)
{
    var dr = AddToDataRow(...);
    table.Rows.Add(dr);
}
else
{
    LogInfo(context, "WARN", "Bad data from API: ICCID/IMEI/MSISDN cannot empty!");
}
```

---

### 12) What is the retry setup for Polly (attempts, delay)?
- SQL transient: 3 attempts by default via `GetSqlTransientPolicy`.

```23:31:/workspace/RetryPolicyHelper.cs
public const int SQL_TRANSIENT_RETRY_MAX_COUNT = 3;
public static ISyncPolicy GetSqlTransientPolicy(IKeysysLogger logger, List<string> errorMessages, int retryCount = SQL_TRANSIENT_RETRY_MAX_COUNT)
{
    var fallbackPolicy = GetFallbackPolicy(errorMessages);
    return fallbackPolicy.Wrap(GetSqlPolicy(logger, retryCount));
}
```

- Backoff: exponential seconds via `CalculateRetryDelay`.

```208:211:/workspace/RetryPolicyHelper.cs
public static TimeSpan CalculateRetryDelay(int retryAttempt)
{
    return TimeSpan.FromSeconds(Math.Pow(CommonConstants.API_ERROR_DELAY_IN_SECONDS, retryAttempt));
}
```

- API calls in this Lambda: `HttpClient` direct (no Polly), but operational retries are handled by SQS re-enqueue (below).

---

### 13) How is re-enqueuing handled for incomplete or timed-out device lists?
- Continues pagination by re-enqueueing with `HasMoreData`, `LargestDeviceIdSeen`, `CurrentServiceProviderId`; resets `RetryCount` when continuing normally.

```247:252:/workspace/AltaworxThingSpaceAWSGetDevices.cs
if (!isLastCycle || sqsValues.HasMoreData)
{
    sqsValues.RetryCount = 0;
    SendMessageToQueue(context, sqsValues, ThingSpaceDestinationQueueGetDevicesURL);
}
```

- On exceptions:
  - If message contains `EXCEPTION`: retries up to `MAX_RETRY_COUNT` (3) by re-queueing.
  - If message contains `EXPIRED`: re-queue immediately to refresh token.

```155:176:/workspace/AltaworxThingSpaceAWSGetDevices.cs
catch (Exception e)
{
    if (e.Message.Contains("EXCEPTION"))
    {
        if (sqsValues.RetryCount < MAX_RETRY_COUNT)
        {
            sqsValues.RetryCount++;
            LogInfo(context, "Requeue Get Values", $"Requeueing with retry count {sqsValues.RetryCount}");
            SendMessageToQueue(context, sqsValues, ThingSpaceDestinationQueueGetDevicesURL);
            return;
        }
        else { ... throw e; }
    }
    else if (e.Message.Contains("EXPIRED"))
    {
        LogInfo(context, "Requeue Get Values", $"Renew credential because it will expire in 60s");
        SendMessageToQueue(context, sqsValues, ThingSpaceDestinationQueueGetDevicesURL);
        return;
    }
    else { ... throw e; }
}
```

- Message attributes used for stateless continuation:

```537:558:/workspace/AltaworxThingSpaceAWSGetDevices.cs
MessageAttributes = new Dictionary<string, MessageAttributeValue>
{
    { "HasMoreData", new MessageAttributeValue { DataType = "String", StringValue = sqsValues.HasMoreData ? "true" : "false"} },
    { "LargestDeviceIdSeen", new MessageAttributeValue { DataType = "String", StringValue = sqsValues.LargestDeviceIdSeen} },
    { "CurrentServiceProviderId", new MessageAttributeValue { DataType = "String", StringValue = sqsValues.CurrentServiceProviderId.ToString()} },
    { "RetryCount", new MessageAttributeValue { DataType = "String", StringValue = sqsValues.RetryCount.ToString()} }
}
```

---

### 14) How do the stored procedures work in the flow?
- After the last page per provider, the Lambda promotes data via two SPs:
  - `usp_ThingSpace_Update_Device(@BillMonthCurrent, @BillYearCurrent, @ServiceProviderId)`
  - `usp_ThingSpace_Update_DeviceDetail(@ServiceProviderId)`

```483:516:/workspace/AltaworxThingSpaceAWSGetDevices.cs
using (var command = new SqlCommand("usp_ThingSpace_Update_Device", connection)) { ... }
...
using (var command = new SqlCommand("usp_ThingSpace_Update_DeviceDetail", connection)) { ... }
```

Behavior (from your SQL):
- `usp_ThingSpace_Update_Device`:
  - MERGE latest rows (per `ICCID`) from `ThingSpaceDeviceStaging` into `ThingSpaceDevice`.
  - Insert new devices; update existing; mark as `Unknown` if missing from staging (except Inventory status).
  - Propagate Unknown status to `ThingSpaceDeviceUsage`.
  - Insert summary pivot into `ThingSpaceDeviceSyncAudit` using the bill year/month provided by the Lambda.

- `usp_ThingSpace_Update_DeviceDetail`:
  - Record last sync count/time in `ThingSpaceDeviceDetailLastSyncDate`.
  - Insert missing `ThingSpaceDeviceDetail` rows from staging/device join.
  - Update changed detail fields; keep Jasper-consistent timestamps.
  - Append a snapshot to `ThingSpaceDeviceDetail_History`.

Billing period parameters come from:

```485:497:/workspace/AltaworxThingSpaceAWSGetDevices.cs
var billingPeriod = ThingSpaceCommon.GetBillingPeriod(context, serviceProviderId, DateTime.UtcNow, TimeZoneInfo.Utc);
command.Parameters.AddWithValue("@BillMonthCurrent", billingPeriod.BillingPeriodMonth);
command.Parameters.AddWithValue("@BillYearCurrent", billingPeriod.BillingPeriodYear);
```

---

### 15) What details are captured in the summary logs?
- Start/end, message attributes, queue targets, and stored-procedure completion.

```62:70:/workspace/AltaworxThingSpaceAWSGetDevices.cs
LogInfo(keysysContext, "STATUS", $"ThingSpaceGetDevices::Beginning to process {processedRecordCount} records...");
LogInfo(keysysContext, "MessageId", record.MessageId);
LogInfo(keysysContext, "EventSource", record.EventSource);
LogInfo(keysysContext, "Body", record.Body);
```

```520:569:/workspace/AltaworxThingSpaceAWSGetDevices.cs
LogInfo(context, "HasMoreData", sqsValues.HasMoreData);
LogInfo(context, "LargestDeviceIdSeen", sqsValues.LargestDeviceIdSeen);
LogInfo(context, "ThingSpaceDestinationQueueGetDevicesURL", thingSpaceDestinationQueueGetDevicesURL);
...
LogInfo(context, "QueueURL", request.QueueUrl);
LogInfo(context, "HasMoreData", request.MessageAttributes["HasMoreData"].StringValue);
LogInfo(context, "LargestDeviceIdSeen", request.MessageAttributes["LargestDeviceIdSeen"].StringValue);
LogInfo(context, "RetryCount", request.MessageAttributes["RetryCount"].StringValue);
```

```233:236:/workspace/AltaworxThingSpaceAWSGetDevices.cs
UpdateThingSpaceDevicesWithPolicy(...);
LogInfo(context, "STATUS", "ThingSpace Devices update done through Stored Procedure");
```

---

### 16) How are reference items (functions, queues, procedures) used in the flow?
- Environment variables:

```31:36:/workspace/AltaworxThingSpaceAWSGetDevices.cs
private string ThingSpaceDevicesGetURL = Environment.GetEnvironmentVariable("ThingSpaceDevicesGetURL");
private int MaxCyclesToProcess = Convert.ToInt32(Environment.GetEnvironmentVariable("MaxCyclesToProcess"));
private string ThingSpaceDestinationQueueGetDevicesURL = Environment.GetEnvironmentVariable("ThingSpaceDestinationQueueGetDevicesURL");
private string DeviceUsageQueueURL = Environment.GetEnvironmentVariable("DeviceUsageQueueURL");
```

- Core HTTP flow (ThingSpace): access token → session token → device list by account with `largestDeviceIdSeen`.

```313:353:/workspace/AltaworxThingSpaceAWSGetDevices.cs
var accessToken = ThingSpaceCommon.GetAccessToken(thingSpaceAuth);
var sessionToken = ThingSpaceCommon.GetSessionToken(thingSpaceAuth, accessToken);
string jsonAccountDeviceContent = "{\"accountName\": \"" + thingSpaceAcctNumber + "\", \"largestDeviceIdSeen\": " + sqsValues.LargestDeviceIdSeen + "}";
var responseAccounDevice = client.PostAsync(ThingSpaceDevicesGetURL, contAccountDevice);
```

- Data movement:
  - Stage: `SqlBulkCopy(..., "ThingSpaceDeviceStaging")`
  - Promote: `usp_ThingSpace_Update_Device`, `usp_ThingSpace_Update_DeviceDetail`
  - Next phase: enqueue to `DeviceUsageQueueURL`

```576:617:/workspace/AltaworxThingSpaceAWSGetDevices.cs
SendMessageToGetDeviceUsageQueue(context, DeviceUsageQueueURL, serviceProviderId);
MessageAttributes: InitializeProcessing=true, GroupNumber=0, ServiceProviderId
```

---

### 17) Can you detail all Lambdas for all carriers?
- Out of scope here. This document focuses on the ThingSpace Lambda only; Telegence and other carrier-specific behaviors are not present in this code path.

---

## Appendix A: Token expiry handling and loop limiter

```316:414:/workspace/AltaworxThingSpaceAWSGetDevices.cs
if (accessToken.Expires_In > EXPIRED_TIME) { ... }
else { throw new Exception($"EXPIRED : AccessToken will expire in 60s, Retry to get new access token"); }
```

```142:148:/workspace/AltaworxThingSpaceAWSGetDevices.cs
while (sqsValues.CycleCounter <= MaxCyclesToProcess)
{
    if (!isLastCycle)
    {
        isLastCycle = GetThingSpaceDevices(context, sqsValues);
        sqsValues.CycleCounter++;
    }
}
```

## Appendix B: Key message attributes for pagination
- `HasMoreData`: string "true"/"false"
- `LargestDeviceIdSeen`: string DeviceId from last record
- `CurrentServiceProviderId`: string provider id
- `RetryCount`: string retry counter for exception flow

## Appendix C: Summary of stored procedures and responsibilities
- `usp_ThingSpace_Truncate_DeviceAndUsageStaging`: truncates device and usage staging tables.
- `usp_ThingSpace_Update_Device`: merges staging into device, handles Unknown, updates usage status, writes audit.
- `usp_ThingSpace_Update_DeviceDetail`: inserts/updates detail and writes history and last sync info.

