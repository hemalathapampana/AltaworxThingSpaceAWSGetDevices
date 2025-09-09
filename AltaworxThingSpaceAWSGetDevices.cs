using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Models;
using Altaworx.ThingSpace.Core;
using Altaworx.ThingSpace.Core.Models;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Helpers;
using Amop.Core.Logger;
using Amop.Core.Models;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using static Amazon.Lambda.SQSEvents.SQSEvent;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxThingSpaceAWSGetDevices
{
    public class Function : AwsFunctionBase
    {
        private string ThingSpaceDevicesGetURL = Environment.GetEnvironmentVariable("ThingSpaceDevicesGetURL");
        private int MaxCyclesToProcess = Convert.ToInt32(Environment.GetEnvironmentVariable("MaxCyclesToProcess"));
        private string ThingSpaceDestinationQueueGetDevicesURL = Environment.GetEnvironmentVariable("ThingSpaceDestinationQueueGetDevicesURL");
        private string DeviceUsageQueueURL = Environment.GetEnvironmentVariable("DeviceUsageQueueURL");
        private int MAX_RETRY_COUNT = 3;
        private int EXPIRED_TIME = 60;
        /// <summary>
        /// Function entry point
        /// </summary>
        /// <param name="sqsEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public void FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            try
            {
                keysysContext = BaseFunctionHandler(context);

                if (string.IsNullOrEmpty(ThingSpaceDevicesGetURL))
                {
                    ThingSpaceDevicesGetURL = context.ClientContext.Environment["ThingSpaceDevicesGetURL"];
                    ThingSpaceDestinationQueueGetDevicesURL = context.ClientContext.Environment["ThingSpaceDestinationQueueGetDevicesURL"];
                    DeviceUsageQueueURL = context.ClientContext.Environment["DeviceUsageQueueURL"];
                    MaxCyclesToProcess = Convert.ToInt32(context.ClientContext.Environment["MaxCyclesToProcess"]);
                }

                int processedRecordCount;
                if (sqsEvent != null && sqsEvent.Records != null)
                {
                    processedRecordCount = sqsEvent.Records.Count;
                    LogInfo(keysysContext, "STATUS", $"ThingSpaceGetDevices::Beginning to process {processedRecordCount} records...");
                    foreach (var record in sqsEvent.Records)
                    {
                        LogInfo(keysysContext, "MessageId", record.MessageId);
                        LogInfo(keysysContext, "EventSource", record.EventSource);
                        LogInfo(keysysContext, "Body", record.Body);

                        var sqsValues = getMessageQueueValues(keysysContext, record); // Sets LargestDeviceIdSeen value. 

                        TryProcessDeviceList(keysysContext, sqsValues);
                    }
                }
                else
                {
                    var sqsValues = new GetDeviceSqsValues();
                    processedRecordCount = 1;

                    TryProcessDeviceList(keysysContext, sqsValues);
                }

                LogInfo(keysysContext, "STATUS", $"Processed {processedRecordCount} records.");
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"EXCEPTION : {ex.Message}");
            }

            CleanUp(keysysContext);
        }

        private void TryProcessDeviceList(KeySysLambdaContext context, GetDeviceSqsValues sqsValues)
        {
            try
            {
                bool proceed = true;
                if (sqsValues.CurrentServiceProviderId == 0) /* 0: initial value*/
                {
                    var serviceProvider = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, IntegrationType.ThingSpace, sqsValues.CurrentServiceProviderId);
                    switch (serviceProvider)
                    {
                        case 0: /* Exception */
                            LogInfo(context, "WARNING", $"Error Getting a service provider id for ThingSpace CurrentServiceProviderId: {sqsValues.CurrentServiceProviderId}");
                            proceed = false;
                            break;
                        case -1:
                            LogInfo(context, "WARNING", $"No Authentication record was found for ThingSpace Service Provider.");
                            proceed = false;
                            break;
                        default:
                            TruncateThingSpaceDeviceAndUsageStagingWithPolicy(context.CentralDbConnectionString, context.logger);
                            sqsValues.CurrentServiceProviderId = serviceProvider;
                            break;
                    }
                }

                if (proceed)
                {
                    ProcessDeviceList(context, sqsValues);
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION:ThingSpaceGetDevices:", ex.Message);
            }
        }

        private GetDeviceSqsValues getMessageQueueValues(KeySysLambdaContext context, SQSMessage message)
        {
            return new GetDeviceSqsValues(context, message);
        }

        private void ProcessDeviceList(KeySysLambdaContext context, GetDeviceSqsValues sqsValues)
        {
            LogInfo(context, "LargestDeviceIdSeen", sqsValues.LargestDeviceIdSeen);

            bool isLastCycle = false;
            //try catch here for exception of "account lsit not found"
            //retry count to sqs values
            try
            {
                while (sqsValues.CycleCounter <= MaxCyclesToProcess) // Keeping "MaxCyclesToProcess" in case "isLastCycle" does not get set from API response.  
                {
                    if (!isLastCycle)
                    {
                        isLastCycle = GetThingSpaceDevices(context, sqsValues);
                        sqsValues.CycleCounter++;
                    }
                    else
                    {
                        break;
                    }
                }
            }
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
                    else
                    {
                        LogInfo(context, "Max retryCount", $"Reached maximum retry count of {MAX_RETRY_COUNT}, considered as failed");
                        throw e;
                    }
                }
                else if (e.Message.Contains("EXPIRED"))
                {
                    LogInfo(context, "Requeue Get Values", $"Renew credential because it will expire in 60s");
                    SendMessageToQueue(context, sqsValues, ThingSpaceDestinationQueueGetDevicesURL);
                    return;
                }
                else
                {
                    LogInfo(context, "Unexpected Exception", $"Considered as critical, no retry executed");
                    throw e;
                }
            }
            sqsValues.CycleCounter = 1;

            LogInfo(context, "isLastCycle", isLastCycle);

            DataTable table = new DataTable();
            table.Columns.Add("ID");
            table.Columns.Add("ICCID");
            table.Columns.Add("IMSI");
            table.Columns.Add("MSISDN");
            table.Columns.Add("IMEI");
            table.Columns.Add("Status");
            table.Columns.Add("RatePlan");
            table.Columns.Add("AccountNumber");
            table.Columns.Add("CreatedBy");
            table.Columns.Add("DeviceCreatedDate");
            table.Columns.Add("LastActivationDate");
            table.Columns.Add("LastUsageDate");
            table.Columns.Add("BillingCycleEndDate");
            table.Columns.Add("ServiceProviderId");
            table.Columns.Add("CreatedDate");
            table.Columns.Add(nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseFirstName));
            table.Columns.Add(nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseMiddleName));
            table.Columns.Add(nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseLastName));
            table.Columns.Add("ThingSpacePPU");
            table.Columns.Add("IPAddress");

            if (sqsValues.ThingSpaceDeviceList.Count > 0)
            {
                var billingPeriod = ThingSpaceCommon.GetBillingPeriod(context, sqsValues.CurrentServiceProviderId, DateTime.UtcNow, TimeZoneInfo.Utc);
                foreach (var thingSpaceDevice in sqsValues.ThingSpaceDeviceList)
                {
                    if (thingSpaceDevice.deviceIds != null && thingSpaceDevice.deviceIds.Count > 0)
                    {
                        var dr = AddToDataRow(sqsValues, table, thingSpaceDevice, billingPeriod.BillingPeriodEnd);
                        table.Rows.Add(dr);
                    }
                    else
                    {
                        LogInfo(context, "WARN", "Bad data from API: ICCID/IMEI/MSISDN cannot empty!");
                    }
                }
                LogInfo(context, "STATUS", "SQL Bulk Copy Start");

                SqlBulkCopy(context, context.CentralDbConnectionString, table, "ThingSpaceDeviceStaging");
            }

            // Check to see if there is another service provider to procss.
            if (isLastCycle)
            {
                UpdateThingSpaceDevicesWithPolicy(context, sqsValues.CurrentServiceProviderId, context.CentralDbConnectionString, context.logger);
                LogInfo(context, "STATUS", "ThingSpace Devices update done through Stored Procedure");

                SendMessageToGetDeviceUsageQueue(context, DeviceUsageQueueURL, sqsValues.CurrentServiceProviderId);

                var nextServiceProvider = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, IntegrationType.ThingSpace, sqsValues.CurrentServiceProviderId);
                if (nextServiceProvider > 0)
                {
                    sqsValues.CurrentServiceProviderId = nextServiceProvider;
                    sqsValues.HasMoreData = true;
                    sqsValues.LargestDeviceIdSeen = "0";
                }
            }

            if (!isLastCycle || sqsValues.HasMoreData)
            {
                //reset retry count
                sqsValues.RetryCount = 0;
                SendMessageToQueue(context, sqsValues, ThingSpaceDestinationQueueGetDevicesURL);
            }
        }

        private DataRow AddToDataRow(GetDeviceSqsValues sqsValues, DataTable table, DeviceResponse device, DateTime billingCycleEndDate)
        {
            var dr = table.NewRow();

            var thingSpacePPU = new ThingSpacePPU()
            {
                FirstName = GetExtendedAttributesByKey(device.extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseFirstName)),
                LastName = GetExtendedAttributesByKey(device.extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseLastName)),
                AddressLine = GetExtendedAttributesByKey(device.extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseAddressLine1)),
                City = GetExtendedAttributesByKey(device.extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseCity)),
                State = GetExtendedAttributesByKey(device.extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseState)),
                ZipCode = GetExtendedAttributesByKey(device.extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseZipCode)),
                Country = GetExtendedAttributesByKey(device.extendedAttributes, nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseCountry)),
            };

            var thingSpacePPUJSON = JsonConvert.SerializeObject(thingSpacePPU);

            dr[1] = device.deviceIds.Where(x => x.kind == "iccId").Select(i => i.id).FirstOrDefault();
            dr[2] = device.deviceIds.Where(x => x.kind == "imsi").Select(i => i.id).FirstOrDefault();
            dr[3] = device.deviceIds.Where(x => x.kind == "msisdn").Select(i => i.id).FirstOrDefault();
            dr[4] = device.deviceIds.Where(x => x.kind == "imei").Select(i => i.id).FirstOrDefault();
            dr[5] = device.carrierInformations.Select(x => x.state).FirstOrDefault();
            dr[6] = device.carrierInformations.Select(x => x.servicePlan).FirstOrDefault();
            dr[7] = device.accountName;
            dr[8] = "AWS Lambda - Get Devices Service";
            dr[9] = device.createdAt;
            dr[10] = device.lastActivationDate;
            dr[11] = device.lastConnectionDate;
            dr[12] = billingCycleEndDate;
            dr[13] = sqsValues.CurrentServiceProviderId;
            dr[14] = DateTime.UtcNow;
            dr[15] = device.extendedAttributes.Where(x => x.key.Contains(nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseFirstName))).Select(x => x.value).FirstOrDefault();
            dr[16] = device.extendedAttributes.Where(x => x.key.Contains(nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseMiddleName))).Select(x => x.value).FirstOrDefault();
            dr[17] = device.extendedAttributes.Where(x => x.key.Contains(nameof(ThingSpaceExtendedAttributeKey.PrimaryPlaceOfUseLastName))).Select(x => x.value).FirstOrDefault();
            dr[18] = thingSpacePPUJSON;
            dr[19] = device.ipAddress;
            return dr;
        }

        private string GetExtendedAttributesByKey(List<ExtendedAttribute> extendedAttributes, string key)
        {
            return extendedAttributes.Where(x => x.key.Contains(key)).Select(x => x.value).FirstOrDefault();
        }

        private bool GetThingSpaceDevices(KeySysLambdaContext context, GetDeviceSqsValues sqsValues)
        {
            bool isLastCycle;
            var thingSpaceAuth = ThingSpaceCommon.GetThingspaceAuthenticationInformation(context.CentralDbConnectionString, sqsValues.CurrentServiceProviderId);

            if (thingSpaceAuth == null)
            {
                return true; /* "True" indicates the last cycle and get it out of the processing loop. */
            }

            LogInfo(context, "GetThingSpaceDevices::ThingSpaceAPIUsername", thingSpaceAuth.Username);
            bool needAccountNumber = string.IsNullOrEmpty(thingSpaceAuth.AccountNumber);
            string thingSpaceAcctNumber = !needAccountNumber ? thingSpaceAuth.AccountNumber : null;

            var accessToken = ThingSpaceCommon.GetAccessToken(thingSpaceAuth);
            if (accessToken != null)
            {
                if (accessToken.Expires_In > EXPIRED_TIME)
                {
                    var sessionToken = ThingSpaceCommon.GetSessionToken(thingSpaceAuth, accessToken);
                    if (sessionToken != null)
                    {
                        if (needAccountNumber)
                        {
                            thingSpaceAcctNumber = ThingSpaceCommon.GetAccountNumber(thingSpaceAuth, accessToken, sessionToken, ThingSpaceDevicesGetURL);
                            if (!string.IsNullOrWhiteSpace(thingSpaceAcctNumber))
                            {
                                SetThingspaceAuthenticationAccountNumber(context, thingSpaceAuth.ThingSpaceAuthenticationId, thingSpaceAcctNumber);
                            }
                        }

                        if (string.IsNullOrWhiteSpace(thingSpaceAcctNumber))
                        {
                            var errorMessage = $"ThingSpaceGetDevices:Call to {ThingSpaceDevicesGetURL} failed. No Account Number Found";

                            LogInfo(context, "ThingSpaceGetDevices", errorMessage);
                            isLastCycle = true;
                            throw new Exception($"EXCEPTION : {errorMessage}");
                        }
                        else
                        {
                            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls13 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;
                            using (HttpClient client = new HttpClient(new LambdaLoggingHandler()))
                            {
                                client.BaseAddress = new Uri(thingSpaceAuth.BaseUrl);

                                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken.Access_Token);
                                client.DefaultRequestHeaders.Add("Accept", "application/json");
                                client.DefaultRequestHeaders.Add("VZ-M2M-Token", sessionToken.sessionToken);

                                string jsonAccountDeviceContent = "{\"accountName\": \"" + thingSpaceAcctNumber + "\", \"largestDeviceIdSeen\": " + sqsValues.LargestDeviceIdSeen + "}";
                                var contAccountDevice = new StringContent(jsonAccountDeviceContent, Encoding.UTF8, "application/json");

                                var responseAccounDevice = client.PostAsync(ThingSpaceDevicesGetURL, contAccountDevice);
                                responseAccounDevice.Wait();
                                if (responseAccounDevice.Result.IsSuccessStatusCode)
                                {
                                    var deviceAccountResult = responseAccounDevice.Result.Content.ReadAsStringAsync().Result;
                                    var deviceList = JsonConvert.DeserializeObject<ThingSpaceDeviceResponseRootObject>(deviceAccountResult);
                                    int deviceTotal = deviceList != null && deviceList.devices != null ? deviceList.devices.Count : 0;

                                    sqsValues.HasMoreData = deviceTotal != 0;
                                    isLastCycle = !sqsValues.HasMoreData;
                                    if (deviceList?.devices != null)
                                    {
                                        for (int i = 0; i < deviceTotal; i++)
                                        {
                                            sqsValues.ThingSpaceDeviceList.Add(deviceList.devices[i]);

                                            if (i == deviceTotal - 1) /* Retrieve hasMoreData and LargestDeviceIdSeen from the last record returned. */
                                            {
                                                sqsValues.HasMoreData = deviceList.hasMoreData.ToString().ToLower() == "true";
                                                if (sqsValues.HasMoreData)
                                                {
                                                    var deviceIdAttrib = deviceList.devices[i].extendedAttributes
                                                        .FirstOrDefault(x => x.key == "DeviceId");
                                                    if (deviceIdAttrib != null)
                                                    {
                                                        sqsValues.LargestDeviceIdSeen = deviceIdAttrib.value;
                                                    }
                                                    else
                                                    {
                                                        // stop syncing
                                                        LogInfo(context, "ThingSpaceGetDevices", "ThingSpaceGetDevices: DeviceId attrib not found.");
                                                    }
                                                }

                                                isLastCycle = !sqsValues.HasMoreData;
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    var errorBodyResult = responseAccounDevice.Result.Content.ReadAsStringAsync().Result;
                                    var errorMessage = $"ThingSpaceGetDevices:Call to {ThingSpaceDevicesGetURL} with AccountNumber failed. {responseAccounDevice.Result.StatusCode} {errorBodyResult}";
                                    LogInfo(context, "ThingSpaceGetDevices", errorMessage);
                                    isLastCycle = true;
                                    throw new Exception($"EXCEPTION : {errorMessage}");
                                }
                            }
                        }
                    }
                    else
                    {
                        LogInfo(context, "EXCEPTION", "No Session Token Returned for Credentials");
                        isLastCycle = true;
                        throw new Exception($"EXCEPTION : No Session Token Returned for Credentials");
                    }
                }
                else
                {
                    LogInfo(context, "INFO", "AccessToken will expire in 60s, Retry to get new access token");
                    isLastCycle = true;
                    throw new Exception($"EXPIRED : AccessToken will expire in 60s, Retry to get new access token");
                }
            }
            else
            {
                LogInfo(context, "EXCEPTION", "No Access Token Returned for Credentials");
                isLastCycle = true;
                throw new Exception($"EXCEPTION : No Access Token Returned for Credentials");
            }

            return isLastCycle;
        }

        private void SetThingspaceAuthenticationAccountNumber(KeySysLambdaContext context, int thingSpaceAuthId, string accountNumber)
        {
            try
            {
                using (var Conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    using (var Cmd = new SqlCommand("usp_ThingSpace_Update_AuthenticationAccountNumber", Conn))
                    {
                        Cmd.CommandType = CommandType.StoredProcedure;
                        Cmd.Parameters.AddWithValue("@accountNumber", accountNumber);
                        Cmd.Parameters.AddWithValue("@thingSpaceAuthenticationId", thingSpaceAuthId);

                        Conn.Open();
                        Cmd.ExecuteNonQuery();

                        Conn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "WARNING", $"Error Saving AccountNumber for ThingSpace Integration_Authentication Id {thingSpaceAuthId}: {ex.GetBaseException().Message}");
            }
        }

        private void TruncateThingSpaceDeviceAndUsageStagingWithPolicy(string connectionString, IKeysysLogger logger)
        {
            logger.LogInfo("SUB", $"TruncateThingSpaceDeviceAndUsageStagingWithPolicy()");
            var errorMessages = new List<string>();
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages);
            sqlTransientRetryPolicy.Execute(() => TruncateThingSpaceDeviceAndUsageStaging(connectionString, logger));
        }

        private void TruncateThingSpaceDeviceAndUsageStaging(string connectionString, IKeysysLogger logger)
        {
            logger.LogInfo("SUB", "TruncateThingSpaceDeviceAndUsageStaging()");
            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand("usp_ThingSpace_Truncate_DeviceAndUsageStaging", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    connection.Open();

                    command.ExecuteNonQuery();
                }
            }
        }

        private void UpdateThingSpaceDevicesWithPolicy(KeySysLambdaContext context, int serviceProviderId, string connectionString,
            IKeysysLogger logger)
        {
            logger.LogInfo("SUB", $"UpdateThingSpaceDevicesWithPolicy({serviceProviderId},...)");
            var errorMessages = new List<string>();
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(logger, errorMessages);
            sqlTransientRetryPolicy.Execute(() => UpdateThingSpaceDevices(context, serviceProviderId, connectionString, logger));
        }

        private void UpdateThingSpaceDevices(KeySysLambdaContext context, int serviceProviderId, string connectionString, IKeysysLogger logger)
        {
            var billingPeriod = ThingSpaceCommon.GetBillingPeriod(context, serviceProviderId, DateTime.UtcNow, TimeZoneInfo.Utc);

            logger.LogInfo("SUB", $"UpdateThingSpaceDevices({serviceProviderId},...)");
            logger.LogInfo("DEBUG", $"usp_ThingSpace_Update_Device, BillMonth: {billingPeriod.BillingPeriodMonth}; BillYear: {billingPeriod.BillingPeriodYear}");

            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand("usp_ThingSpace_Update_Device", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("@BillMonthCurrent", billingPeriod.BillingPeriodMonth);
                    command.Parameters.AddWithValue("@BillYearCurrent", billingPeriod.BillingPeriodYear);
                    command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    connection.Open();

                    command.ExecuteNonQuery();
                }
            }

            logger.LogInfo("DEBUG", "usp_ThingSpace_Update_DeviceDetail");
            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand("usp_ThingSpace_Update_DeviceDetail", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    connection.Open();

                    command.ExecuteNonQuery();
                }
            }
        }

        private void SendMessageToQueue(KeySysLambdaContext context, GetDeviceSqsValues sqsValues, string thingSpaceDestinationQueueGetDevicesURL)
        {
            LogInfo(context, "SUB", "SendMessageToQueue");

            LogInfo(context, "HasMoreData", sqsValues.HasMoreData);
            LogInfo(context, "LargestDeviceIdSeen", sqsValues.LargestDeviceIdSeen);
            LogInfo(context, "ThingSpaceDestinationQueueGetDevicesURL", thingSpaceDestinationQueueGetDevicesURL);

            if (string.IsNullOrWhiteSpace(thingSpaceDestinationQueueGetDevicesURL))
            {
                return; // to be able to skip enqueuing messages in a test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"HasMoreData is {sqsValues.HasMoreData} and LargestDeviceIdSeen is {sqsValues.LargestDeviceIdSeen}";
                LogInfo(context, "Sending message for", $"{requestMsgBody} to destination queue: {thingSpaceDestinationQueueGetDevicesURL}");

                var request = new SendMessageRequest
                {
                    DelaySeconds = (int)TimeSpan.FromSeconds(5).TotalSeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "HasMoreData", new MessageAttributeValue
                            { DataType = "String", StringValue = sqsValues.HasMoreData ? "true" : "false"}
                        },
                        {
                            "LargestDeviceIdSeen", new MessageAttributeValue
                            { DataType = "String", StringValue = sqsValues.LargestDeviceIdSeen}
                        },
                        {
                            "CurrentServiceProviderId", new MessageAttributeValue
                            { DataType = "String", StringValue = sqsValues.CurrentServiceProviderId.ToString()}
                        },
                        {
                            "RetryCount", new MessageAttributeValue
                            { DataType = "String", StringValue = sqsValues.RetryCount.ToString()}
                        }
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = thingSpaceDestinationQueueGetDevicesURL
                };

                LogInfo(context, "STATUS", "SendMessageRequest is ready!");
                LogInfo(context, "MessageBody", request.MessageBody);
                LogInfo(context, "QueueURL", request.QueueUrl);
                LogInfo(context, "HasMoreData", request.MessageAttributes["HasMoreData"].StringValue);
                LogInfo(context, "LargestDeviceIdSeen", request.MessageAttributes["LargestDeviceIdSeen"].StringValue);
                LogInfo(context, "RetryCount", request.MessageAttributes["RetryCount"].StringValue);

                var response = client.SendMessageAsync(request);
                response.Wait();
                LogInfo(context, "RESPONSE STATUS", response.Status);
            }
        }

        private void SendMessageToGetDeviceUsageQueue(KeySysLambdaContext context, string deviceUsageQueueURL, int serviceProviderId)
        {
            LogInfo(context, "SUB", "SendMessageToGetDeviceUsageQueue");
            LogInfo(context, "InitializeProcessing", true);
            LogInfo(context, "DeviceUsageQueueURL", deviceUsageQueueURL);
            LogInfo(context, "ServiceProviderId", serviceProviderId);

            if (string.IsNullOrWhiteSpace(deviceUsageQueueURL))
            {
                return; // to be able to skip enqueuing messages in a test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Requesting email to process";
                LogInfo(context, "Sending message for", $"{requestMsgBody} to DeviceUsage queue: {deviceUsageQueueURL}");

                var request = new SendMessageRequest
                {
                    DelaySeconds = (int)TimeSpan.FromSeconds(5).TotalSeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {
                            "InitializeProcessing", new MessageAttributeValue
                            {
                                DataType = "String", StringValue = true.ToString()
                            }
                        },
                        {
                            "GroupNumber", new MessageAttributeValue
                            {
                                DataType = "String", StringValue = 0.ToString()
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
                    QueueUrl = deviceUsageQueueURL
                };

                LogInfo(context, "MessageBody", request.MessageBody);
                LogInfo(context, "QueueURL", request.QueueUrl);
                LogInfo(context, "InitializeProcessing", request.MessageAttributes["InitializeProcessing"].StringValue);

                var response = client.SendMessageAsync(request);
                response.Wait();
                LogInfo(context, "RESPONSE STATUS", response.Status);
            }
        }
    }
}
