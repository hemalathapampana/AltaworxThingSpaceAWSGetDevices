# Point 4: ThingSpace Carrier API Documentation

## Overview
This document provides comprehensive documentation for integrating with Verizon's ThingSpace IoT platform APIs, including ThingSpace IoT and ThingSpace PN (Private Network) services.

## Base Configuration

### Supported Carriers
- **Verizon ThingSpace IoT**
- **Verizon ThingSpace PN**

### Base URL
```
https://thingspace.verizon.com/
```

### Primary Endpoint
```
/api/m2m/v1/devices/actions/list
```

## Authentication Flow

ThingSpace API uses a two-step authentication process:
1. **OAuth2 Access Token** - Generated using client credentials
2. **Session Token** - Generated using login credentials and access token

### Step 1: OAuth2 Access Token

#### Endpoint
```
POST https://thingspace.verizon.com/api/ts/v1/oauth2/token
```

#### Request Headers
```
Content-Type: application/json
```

#### Request Body
```json
{
  "client_id": "{clientId}",
  "client_secret": "{clientSecret}",
  "grant_type": "client_credentials"
}
```

#### Response Example
```json
{
  "access_token": "eyJraWQiOiJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.xxxxx.yyyyy",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

#### Response Fields
| Field | Type | Description |
|-------|------|-------------|
| `access_token` | string | JWT token for API authentication |
| `token_type` | string | Always "Bearer" |
| `expires_in` | integer | Token expiration time in seconds (3600 = 1 hour) |

### Step 2: Session Token

#### Endpoint
```
POST https://thingspace.verizon.com/api/m2m/v1/session/login
```

#### Request Headers
```
Content-Type: application/json
Authorization: Bearer {access_token}
```

#### Request Body
```json
{
  "username": "{username}",
  "password": "{decoded_password}"
}
```

#### Response Example
```json
{
  "sessionToken": "vzm2m-6d4aa34c-fd34-42b1-8e0e-9ab123456789",
  "expiresIn": 1800
}
```

#### Response Fields
| Field | Type | Description |
|-------|------|-------------|
| `sessionToken` | string | Session identifier for M2M operations |
| `expiresIn` | integer | Token expiration time in seconds (1800 = 30 minutes) |

## Device List API

### Endpoint
```
POST https://thingspace.verizon.com/api/m2m/v1/devices/actions/list
```

### Final Request Headers
```
Authorization: Bearer eyJraWQiOiJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.xxxxx.yyyyy
Accept: application/json
VZ-M2M-Token: vzm2m-6d4aa34c-fd34-42b1-8e0e-9ab123456789
Content-Type: application/json
```

### Request Payload Options

#### Option 1: With Account Name and Largest Device ID
```json
{
  "accountName": "0242390116-00001",
  "largestDeviceIdSeen": 12345
}
```

#### Option 2: Device Query by ICCID
```json
{
  "deviceId": {
    "id": "1234567890123456789",
    "kind": "iccid"
  }
}
```

### Payload Configuration

#### Account Name Source
- **Source**: Database table `Integration_Authentication`
- **Column**: `OAuth2Custom1`
- **Format**: Account identifier (e.g., "0242390116-00001")

#### Largest Device ID Seen
- **Source**: SQS Message Attribute
- **Attribute Key**: "LargestDeviceIdSeen"
- **Type**: Integer
- **Purpose**: Pagination support for large device lists

## Data Sources and Configuration

### Database Configuration
| Field | Source Table | Column | Description |
|-------|--------------|--------|-------------|
| Account Name | `Integration_Authentication` | `OAuth2Custom1` | ThingSpace account identifier |
| Client ID | `Integration_Authentication` | `ClientId` | OAuth2 client identifier |
| Client Secret | `Integration_Authentication` | `ClientSecret` | OAuth2 client secret |
| Username | `Integration_Authentication` | `Username` | ThingSpace login username |
| Password | `Integration_Authentication` | `Password` | ThingSpace login password (encoded) |

### SQS Message Attributes
| Attribute | Type | Purpose |
|-----------|------|---------|
| `LargestDeviceIdSeen` | String | Pagination cursor for device listing |
| `HasMoreData` | String | Boolean flag indicating more data availability |
| `CurrentServiceProviderId` | String | Current service provider being processed |
| `RetryCount` | String | Number of retry attempts for the operation |

## Error Handling

### Common Error Scenarios
1. **Authentication Failures**
   - Invalid client credentials
   - Expired access tokens
   - Invalid session tokens

2. **API Rate Limiting**
   - Too many requests per time window
   - Concurrent request limits

3. **Data Validation Errors**
   - Invalid ICCID format
   - Missing required fields
   - Invalid account names

### Best Practices
1. **Token Management**
   - Cache access tokens until expiration
   - Refresh session tokens before expiration
   - Implement retry logic for token refresh

2. **Error Logging**
   - Log all API responses for debugging
   - Include correlation IDs for tracking
   - Monitor error rates and patterns

3. **Rate Limiting**
   - Implement exponential backoff
   - Respect API rate limits
   - Use batch operations when available

## Security Considerations

1. **Credential Storage**
   - Store credentials securely (encrypted)
   - Use environment variables or secure vaults
   - Rotate credentials regularly

2. **Token Handling**
   - Never log access tokens or session tokens
   - Implement secure token storage
   - Clear tokens from memory after use

3. **TLS Configuration**
   - Use TLS 1.2 or higher
   - Validate SSL certificates
   - Implement certificate pinning if required

## Integration Notes

- **Batch Processing**: Use the `BatchSize` constant (1000) for optimal performance
- **Pagination**: Utilize `largestDeviceIdSeen` for efficient data retrieval
- **Monitoring**: Implement comprehensive logging using `IKeysysLogger`
- **Resilience**: Use `LambdaLoggingHandler` for AWS Lambda integration