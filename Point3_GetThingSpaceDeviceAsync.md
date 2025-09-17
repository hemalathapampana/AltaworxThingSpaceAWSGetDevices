# Point 3: GetThingSpaceDeviceAsync Method Documentation

## Overview
The `GetThingSpaceDeviceAsync` method is responsible for retrieving device information from Verizon's ThingSpace API using an ICCID (Integrated Circuit Card Identifier).

## Method Signature
```csharp
public static async Task<DeviceResponse> GetThingSpaceDeviceAsync(
    string iccid, 
    string baseUrl, 
    ThingSpaceTokenResponse accessToken, 
    ThingSpaceLoginResponse sessionToken, 
    IKeysysLogger logger)
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `iccid` | string | Integrated Circuit Card Identifier for the device |
| `baseUrl` | string | Base URL for the ThingSpace API |
| `accessToken` | ThingSpaceTokenResponse | OAuth2 access token for API authentication |
| `sessionToken` | ThingSpaceLoginResponse | Session token for API authentication |
| `logger` | IKeysysLogger | Logger instance for debugging and monitoring |

## Return Value
- **Type**: `Task<DeviceResponse>`
- **Description**: Returns device information or null if not found/error occurs

## Implementation Details

### Security Protocol Configuration
```csharp
ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls13 | 
                                      SecurityProtocolType.Tls12 | 
                                      SecurityProtocolType.Tls11 | 
                                      SecurityProtocolType.Tls;
```
Configures multiple TLS versions for maximum compatibility.

### HTTP Client Configuration
- **Base Address**: `{baseUrl}/api/m2m/v1/devices/actions/list`
- **Handler**: Uses `LambdaLoggingHandler()` for AWS Lambda integration
- **Timeout**: Uses default HttpClient timeout

### Request Headers
| Header | Value | Purpose |
|--------|-------|---------|
| `Authorization` | `Bearer {accessToken.Access_Token}` | OAuth2 authentication |
| `Accept` | `application/json` | Response format specification |
| `VZ-M2M-Token` | `{sessionToken.sessionToken}` | Verizon session authentication |

### Request Payload
```json
{
    "deviceId": {
        "id": "{iccid}",
        "kind": "iccid"
    }
}
```

### Response Handling

#### Success Response
- **Condition**: `response.IsSuccessStatusCode`
- **Processing**: 
  1. Reads response body as string
  2. Deserializes JSON to `ThingSpaceDeviceResponseRootObject`
  3. Returns first device from the `devices` array

#### Error Response
- **Condition**: Non-success status code
- **Processing**:
  1. Reads error response body
  2. Logs error using `logger.LogInfo("EXCEPTION", responseBody)`
  3. Returns `null`

## Logging
The method includes comprehensive logging:
- Method entry: `"INFO", "GetThingSpaceDeviceAsync"`
- ICCID parameter: `"INFO", $"iccid: {iccid}"`
- API endpoint: `"Endpoint", client.BaseAddress`
- Errors: `"EXCEPTION", responseBody`

## Error Handling
- Returns `null` for any HTTP errors
- Logs error responses for debugging
- Uses `using` statement for proper HttpClient disposal
- Handles JSON deserialization safely with null-conditional operators

## Usage Example
```csharp
var device = await GetThingSpaceDeviceAsync(
    "1234567890123456789", 
    "https://thingspace.verizon.com",
    accessTokenResponse,
    sessionTokenResponse,
    logger
);

if (device != null)
{
    // Process device information
}
```

## Dependencies
- `System.Net.Http`
- `Newtonsoft.Json` (JsonConvert)
- `System.Text` (Encoding)
- Custom types: `DeviceResponse`, `ThingSpaceTokenResponse`, `ThingSpaceLoginResponse`, `ThingSpaceDeviceResponseRootObject`
- `LambdaLoggingHandler`
- `IKeysysLogger`