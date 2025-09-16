## AltaworxThingSpaceAWSGetDevices Lambda Function Flow Documentation

### Overview
- Processes ThingSpace device synchronization per service provider.
- Paginates through the ThingSpace Devices API using `largestDeviceIdSeen` and `hasMoreData`.
- Stages results into `ThingSpaceDeviceStaging`, then promotes to core tables via stored procedures.
- Triggers a follow-up usage processing queue after device sync per provider.

---

## High-Level Sequential Flow
1) Entry Point: FunctionHandler
- Trigger: SQS event. If no records present, runs a single default iteration.
- Flow: Initialize context → process message(s) → clean up.

2) Context Initialization: BaseFunctionHandler (AwsFunctionBase)
- Initializes `KeySysLambdaContext`, loads OU/provider settings, and exposes DB connection strings and logging.

3) Message Processing Decision Point
- This Lambda does not use an `InitializeProcessing` flag/branch. All messages route to device list processing.

4) Processing Branch: ProcessDeviceList
- Retrieves/paginates devices from ThingSpace using account number and `largestDeviceIdSeen`.
- Builds a `DataTable` of device data and bulk-inserts into `ThingSpaceDeviceStaging`.
- On last page for a provider, runs stored procedures to promote data and enqueues usage processing.
- Re-enqueues itself with pagination state when more data remains or to proceed to the next provider.

5) Cleanup: CleanUp (AwsFunctionBase)
- Disposes resources and closes provider-specific context.

---

## Detailed Low-Level Flow
1) FunctionHandler (Main Entry Point)
- Initializes `KeySysLambdaContext` via `BaseFunctionHandler`.
- Loads environment variables: `ThingSpaceDevicesGetURL`, `ThingSpaceDestinationQueueGetDevicesURL`, `DeviceUsageQueueURL`, `MaxCyclesToProcess`.
- Processes each SQS record (or a default run if none). Extracts pagination and provider state from message attributes.
- Error handling: logs exceptions; for API failures uses re-enqueue with capped retry; for token-expiry, re-enqueues to refresh.
- Calls `CleanUp` at the end.

2) BaseFunctionHandler (AwsFunctionBase)
- Creates and returns `KeySysLambdaContext` and loads OU settings.

3) Staging Reset at Run Start
- When starting with no current provider, fetches next provider and truncates ThingSpace device/usage staging via a SQL transient retry policy.

4) ProcessDeviceList (Main Processing Path)
- Authentication: retrieves ThingSpace credentials from DB; obtains access token and session token; if access token is near expiry (< 60s), re-enqueues to refresh.
- Pagination loop: calls ThingSpace Devices API with `accountName` and `largestDeviceIdSeen`. Uses response `hasMoreData` and last item `DeviceId` to continue.
- Data shaping: validates required identifiers; maps status, rate plan, account, dates, primary place of use (PPU) fields, and IP address.
- Bulk insert: writes staged rows into `ThingSpaceDeviceStaging` (bulk copy).
- Provider finalization (last page): executes stored procedures `usp_ThingSpace_Update_Device` and `usp_ThingSpace_Update_DeviceDetail`, then enqueues a message to the device usage queue.
- Continuation: re-enqueues a message to the get-devices queue with `HasMoreData`, `LargestDeviceIdSeen`, `CurrentServiceProviderId`, `RetryCount` for stateless continuation or next provider.

---

## Supporting Functions (Concise)
- Message attribute parsing: reads `HasMoreData`, `LargestDeviceIdSeen`, `CurrentServiceProviderId`, `RetryCount` from SQS attributes; defaults when absent.
- DataTable definition/population: includes identifiers (ICCID, IMSI, MSISDN, IMEI), status, rate plan, account name, device timestamps, billing cycle end date, service provider id, audit fields, PPU JSON, and IP address.
- SQL Bulk Copy: standardized bulk insert with configured timeouts and batch size.
- Re-enqueue mechanics: 5-second delay; includes pagination and provider state.
- Usage kick-off: enqueues a follow-up message to `DeviceUsageQueueURL` once devices are promoted for a provider.

---

## Stored Procedures (How They Fit)
- Truncate staging at start of a provider run:
  - `usp_ThingSpace_Truncate_DeviceAndUsageStaging` (clears device and usage staging tables).
- Promote after last page per provider:
  - `usp_ThingSpace_Update_Device` (MERGE from staging into device; handles Unknown status; updates usage statuses; writes audit by current bill month/year).
  - `usp_ThingSpace_Update_DeviceDetail` (insert/update device detail; record last sync; append to history).

---

## Error Handling, Retries, and Logging
- SQL transient errors: Polly-backed retry for truncation and promotion steps (3 attempts, exponential backoff).
- API faults: handled operationally by SQS re-enqueue with `RetryCount` up to 3; token near-expiry forces re-enqueue.
- Logging: function lifecycle, message attributes, pagination markers, bulk copy start, stored procedure completion, and queue outcomes.

---

## Data Flow Summary
SQS Trigger → Initialize Context → (If no provider set: truncate staging, set provider) → Paginate ThingSpace Devices API → Stage to `ThingSpaceDeviceStaging` → (On last page) Promote via SPs → Enqueue device usage processing → Re-enqueue for next page or provider → Cleanup.

---

## Dependencies and Configuration
- Dependencies: AWS SQS, SQL Server, ThingSpace API, AWS Lambda runtime.
- Configuration: ThingSpace URLs, queue URLs, `MaxCyclesToProcess`, DB connection strings, ThingSpace credentials in DB.

---

## Database Artifacts Touched
- Staging: `ThingSpaceDeviceStaging` (and usage staging cleared at start).
- Core: `ThingSpaceDevice`, `ThingSpaceDeviceDetail`, `ThingSpaceDeviceDetail_History`, `ThingSpaceDeviceUsage`, `ThingSpaceDeviceSyncAudit`.

