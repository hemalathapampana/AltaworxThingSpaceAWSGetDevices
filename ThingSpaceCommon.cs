using System;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Collections.Generic;
using System.Text;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using Amop.Core.Services.Base64Service;
using System.Net;
using System.Threading.Tasks;
using Amop.Core.Logger;
using System.Linq;
using Altaworx.ThingSpace.Core.Models;
using Altaworx.ThingSpace.Core.Models.Api;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Helpers;
using Amop.Core.Constants;
using Amop.Core.Models.DeviceBulkChange;
using Polly;
using Amop.Core.Services.Http;
using Amazon.Runtime;
using Amop.Core.Helpers.Teal;
using Amop.Core.Models.Teal;

namespace Altaworx.ThingSpace.Core
{
    public static class ThingSpaceCommon
    {
        public const int DEFAULT_BILLING_CYCLE_END_DAY = 23;
        public const int DEFAULT_BILLING_CYCLE_END_HOUR = 0;
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

        public static async Task<DeviceChangeResult<string, string>> PutUpdateIdentifierAsync(ThingSpaceAuthentication thingSpaceAuth, string requestJson, ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, string thingSpaceChangeIdentifierURL, IKeysysLogger logger, SingletonHttpClientFactory httpClientFactory, HttpRequestFactory httpRequestFactory)
        {
            logger.LogInfo(CommonConstants.INFO, "");
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls13 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

            var baseAddress = new Uri($"{thingSpaceAuth.BaseUrl.TrimEnd('/')}{thingSpaceChangeIdentifierURL}");
            var requestHeader = new Dictionary<string, string> {
                { CommonConstants.AUTHORIZATION, $"{CommonConstants.BEARER } {accessToken.Access_Token}" },
                { CommonConstants.ACCEPT, CommonConstants.APPLICATION_JSON },
                { CommonConstants.VZ_M2M_TOKEN, sessionToken.sessionToken }
            };
            var contDevice = new StringContent(requestJson, Encoding.UTF8, CommonConstants.APPLICATION_JSON);
            var requestMessage = httpRequestFactory.BuildRequestMessage(null, new HttpMethod(CommonConstants.PUT), baseAddress, requestHeader, contDevice);
            var response = await httpClientFactory.GetClient().SendAsync(requestMessage);
            if (response.IsSuccessStatusCode)
            {
                string responseBody = await response.Content.ReadAsStringAsync();
                var body = JsonConvert.DeserializeObject<ThingSpaceReponseRequestId>(responseBody);
                logger.LogInfo(CommonConstants.INFO, body.RequestId);
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = $"{baseAddress}",
                    HasErrors = false,
                    RequestObject = requestJson,
                    ResponseObject = body.RequestId
                };
            }
            else
            {
                var responseBody = response.Content.ReadAsStringAsync().Result;
                logger.LogInfo(CommonConstants.WARNING, responseBody);
                return new DeviceChangeResult<string, string>()
                {
                    ActionText = $"{baseAddress}",
                    HasErrors = true,
                    RequestObject = requestJson,
                    ResponseObject = responseBody
                };
            }
        }

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

        public static string GetAccountNumber(ThingSpaceAuthentication thingSpaceAuth, ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, string devicesGetUrl)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls13 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;
            string accountNumber = null;
            using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
            {
                client.BaseAddress = new Uri(thingSpaceAuth.BaseUrl);
                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                client.DefaultRequestHeaders.Add("Accept", "application/json");
                client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);

                string jsonDeviceContent = "{\"currentState\": \"Active\"}";
                var contDevice = new StringContent(jsonDeviceContent, Encoding.UTF8, "application/json");

                var responseDevice = client.PostAsync(devicesGetUrl, contDevice);
                responseDevice.Wait();
                if (responseDevice.Result.IsSuccessStatusCode)
                {
                    var deviceResult = responseDevice.Result.Content.ReadAsStringAsync().Result;
                    var deviceList = JsonConvert.DeserializeObject<ThingSpaceDeviceResponseRootObject>(deviceResult);
                    if (deviceList != null && deviceList.devices.Count > 0)
                    {
                        accountNumber = deviceList.devices[0].accountName;
                    }
                }
            }

            return accountNumber;
        }

        public static ThingSpaceAuthentication GetThingspaceAuthenticationInformation(string connectionString, int currentServiceProviderId)
        {
            ThingSpaceAuthentication thingSpaceAuth = null;
            try
            {
                using (var Conn = new SqlConnection(connectionString))
                {
                    using (var Cmd = new SqlCommand("usp_ThingSpace_Get_AuthenticationByProviderId", Conn))
                    {
                        Cmd.CommandType = CommandType.StoredProcedure;
                        Cmd.Parameters.AddWithValue("@providerId", currentServiceProviderId);
                        Conn.Open();

                        SqlDataReader rdr = Cmd.ExecuteReader();
                        while (rdr.Read())
                        {
                            thingSpaceAuth = new ThingSpaceAuthentication()
                            {
                                ThingSpaceAuthenticationId = Convert.ToInt32(rdr["integrationAuthenticationId"]),
                                BaseUrl = rdr["baseUrl"].ToString(),
                                ClientId = rdr["clientid"].ToString(),
                                ClientSecret = rdr["clientSecret"].ToString(),
                                AuthTokenUrl = rdr["authTokenUrl"].ToString(),
                                AuthUrl = rdr["authUrl"].ToString(),
                                Username = rdr["username"].ToString(),
                                Password = rdr["password"].ToString(),
                                AccountNumber = rdr["accountNumber"].ToString(),
                                WriteIsEnabled = rdr.GetBoolean(rdr.GetOrdinal("WriteIsEnabled"))
                            };
                            break;
                        }
                        Conn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return thingSpaceAuth;
        }

        public static string GetThingspaceActivateDeviceBody(ThingSpaceAuthentication thingSpaceAuth, ThingSpaceStatusUpdateRequest request,
             bool isUpdateThingSpacePPU)
        {
            var thingSpaceRequest = new ThingSpaceUpdateDeviceStatusRequest
            {
                Devices = new[]
                {
                    new ThingSpaceDeviceRequest
                    {
                        DeviceIds = new List<ThingSpaceDeviceId>()
                        {
                            new ThingSpaceDeviceId
                            {
                                Kind = nameof(ThingSpaceExtendedAttributeKey.iccid),
                                Id = request.ICCID
                            },
                            new ThingSpaceDeviceId
                            {
                                Kind = nameof(ThingSpaceExtendedAttributeKey.imei),
                                Id = request.IMEI
                            }
                        }
                    }
                },
                AccountName = thingSpaceAuth.AccountNumber,
                ServicePlan = request.RatePlanCode,
                PublicIpRestriction = request?.PublicIpRestriction
            };
            if (!string.IsNullOrEmpty(request.MdnZipCode))
            {
                thingSpaceRequest.MdnZipCode = request.MdnZipCode;
            }
            else
            {
                thingSpaceRequest.MdnZipCode = request?.thingSpacePPU.ZipCode;
            }

            if (isUpdateThingSpacePPU && request.thingSpacePPU != null)
            {
                thingSpaceRequest.PrimaryPlaceOfUse = new ThingSpacePrimaryPlaceOfUse()
                {
                    Customer = new ThingSpaceCustomerName()
                    {
                        FirstName = request.thingSpacePPU.FirstName,
                        LastName = request.thingSpacePPU.LastName
                    },
                    Address = new ThingSpaceAddress()
                    {
                        AddressLine1 = request.thingSpacePPU.AddressLine,
                        City = request.thingSpacePPU.City,
                        State = request.thingSpacePPU.State,
                        Zip = request.thingSpacePPU.ZipCode,
                        Country = request.thingSpacePPU.Country
                    }
                };

            }

            return JsonConvert.SerializeObject(thingSpaceRequest, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            });
        }

        public static async Task<string> GetStatusRequest(ThingSpaceAuthentication thingSpaceAuth, ThingSpaceTokenResponse accessToken, ThingSpaceLoginResponse sessionToken, string requestId, string getStatusUrl)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls13 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;
            using (var client = new HttpClient(new LambdaLoggingHandler()))
            {
                client.BaseAddress = new Uri($"{thingSpaceAuth.BaseUrl.TrimEnd('/')}{string.Format(getStatusUrl, thingSpaceAuth.AccountNumber, requestId)}");
                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                client.DefaultRequestHeaders.Add("Accept", "application/json");
                client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);
                var response = await client.GetAsync(client.BaseAddress);
                if (response.IsSuccessStatusCode)
                {
                    string responseBody = await response.Content.ReadAsStringAsync();
                    var body = JsonConvert.DeserializeObject<ThingSpaceStatusRequest>(responseBody);
                    return body?.Status;
                }
                else
                {
                    return null;
                }
            }
        }

        // For ThingSpace, the timeZoneInfo should be UTC since this DateTime will be saved to the database as UTC
        public static BillingPeriod GetBillingPeriod(KeySysLambdaContext context, int serviceProviderId, DateTime currentDateTime, TimeZoneInfo timeZoneInfo)
        {
            AwsFunctionBase.LogInfo(context, CommonConstants.SUB, $"({serviceProviderId}, {currentDateTime}, {timeZoneInfo.DisplayName})");

            //get billing end day and bill end hour in serviceProvider 
            var centralDbConnectionString = context.CentralDbConnectionString;
            var serviceProvider = ServiceProviderCommon.GetServiceProvider(centralDbConnectionString, serviceProviderId);
            var billingPeriodYear = currentDateTime.Year;
            var billingPeriodMonth = currentDateTime.Month;
            var billCycleEndDay = serviceProvider.BillPeriodEndDay ?? DEFAULT_BILLING_CYCLE_END_DAY;
            var billCycleEndHour = serviceProvider.BillPeriodEndHour ?? DEFAULT_BILLING_CYCLE_END_HOUR;

            var billingPeriod = new BillingPeriod(0, serviceProviderId, billingPeriodYear, billingPeriodMonth, billCycleEndDay, billCycleEndHour, timeZoneInfo);

            // if billing end day < current day, add 1 month
            if (billingPeriod.BillingPeriodEndDay < currentDateTime.Day)
            {
                var endDate = billingPeriod.BillingPeriodEnd.AddMonths(1);
                billingPeriodYear = endDate.Year;
                billingPeriodMonth = endDate.Month;
                billingPeriod = new BillingPeriod(0, serviceProviderId, billingPeriodYear, billingPeriodMonth, billCycleEndDay, billCycleEndHour, timeZoneInfo);
            }

            var existingBillingPeriod = BillingPeriodHelper.GetBillingPeriodForServiceProviderByCurrentDate((type, content) => AwsFunctionBase.LogInfo(context, type, content), centralDbConnectionString, serviceProviderId, currentDateTime, timeZoneInfo, billingPeriodYear, billingPeriodMonth, billCycleEndDay, billCycleEndHour);
            if (existingBillingPeriod != null)
            {
                billingPeriod = existingBillingPeriod;
            }
            else
            {
                AwsFunctionBase.LogInfo(context, CommonConstants.INFO, $"No Existing Billing period found. Creating a new billing period.");
            }
            AwsFunctionBase.LogInfo(context, CommonConstants.INFO, $"BillingPeriodEndDay: {billingPeriod.BillingPeriodEnd}");
            return billingPeriod;
        }
    }
}
