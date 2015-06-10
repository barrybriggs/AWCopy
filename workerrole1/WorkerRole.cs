/********************************************************************************************************
 *  AWCOPY:  COPY S3 OBJECTS TO AZURE BLOBS
 * 
 *  Copyright (c) Barry Briggs
 *  All Rights Reserved 
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the 
 *  License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *  THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, 
 *  INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE, 
 *  MERCHANTABLITY OR NON-INFRINGEMENT. 
 *  
 *  See the Apache 2 License for the specific language governing permissions and limitations under the License. 
 *
 * 
 * 
 * ******************************************************************************************************/
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;

using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.Storage;

using Orleans;
using Orleans.Runtime;
using Orleans.Host;


namespace WorkerRoleWithSBQueue1
{
    public class WorkerRole : RoleEntryPoint
    {

        private OrleansAzureSilo orleansAzureSilo;

        protected static bool collectPerfCounters = true;
        protected static bool collectWindowsEventLogs = false;
        protected static bool fullCrashDumps = false;

        // The name of your queue
        const string QueueName = "ProcessingQueue";

        // QueueClient is thread-safe. Recommended that you cache 
        // rather than recreating it on every request
        QueueClient Client;
        ManualResetEvent CompletedEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.WriteLine("Starting processing of messages");

            // Initiates the message pump and callback is invoked for each message that is received, calling close on the client will stop the pump.
            Client.OnMessage((receivedMessage) =>
                {
                    try
                    {
                        // Process the message
                        Trace.WriteLine("Processing Service Bus message: " + receivedMessage.SequenceNumber.ToString());
                    }
                    catch
                    {
                        // Handle any message processing specific exceptions here
                    }
                });

            CompletedEvent.WaitOne();
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections 
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.
            RoleEnvironment.Changing += RoleEnvironmentChanging;
            SetupEnvironmentChangeHandlers();

            DiagnosticMonitorConfiguration diagConfig = ConfigureDiagnostics();

            // Start the diagnostic monitor. 
            // The parameter references a connection string specified in the service configuration file 
            // that indicates the storage account where diagnostic information will be transferred. 
            // If the value of this setting is "UseDevelopmentStorage=true" then logs are written to development storage.
            DiagnosticMonitor.Start("Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString", diagConfig);

            orleansAzureSilo = new OrleansAzureSilo();

            // Create the queue if it does not exist already
            string connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            if (!namespaceManager.QueueExists(QueueName))
            {
                namespaceManager.CreateQueue(QueueName);
            }

            // Initialize the connection to Service Bus Queue
            Client = QueueClient.CreateFromConnectionString(connectionString, QueueName);
            bool ok= base.OnStart();
            if (ok)
            {

                ok = orleansAzureSilo.Start(RoleEnvironment.DeploymentId, RoleEnvironment.CurrentRoleInstance);
            }
            return ok;
        }

        public override void OnStop()
        {
            // Close the connection to Service Bus Queue
            Client.Close();
            CompletedEvent.Set();
            base.OnStop();
        }
        public static DiagnosticMonitorConfiguration ConfigureDiagnostics()
        {
            // Get default initial configuration.
            DiagnosticMonitorConfiguration diagConfig = DiagnosticMonitor.GetDefaultInitialConfiguration();

            // Add performance counters to the diagnostic configuration
            if (collectPerfCounters)
            {
                diagConfig.PerformanceCounters.DataSources.Add(
                    new PerformanceCounterConfiguration
                    {
                        CounterSpecifier = @"\Processor(_Total)\% Processor Time",
                        SampleRate = TimeSpan.FromSeconds(5)
                    });
                diagConfig.PerformanceCounters.DataSources.Add(
                    new PerformanceCounterConfiguration
                    {
                        CounterSpecifier = @"\Memory\Available Mbytes",
                        SampleRate = TimeSpan.FromSeconds(5)
                    });
            }

            // Add event collection from the Windows Event Log
            if (collectWindowsEventLogs)
            {
                diagConfig.WindowsEventLog.DataSources.Add("System!*");
                diagConfig.WindowsEventLog.DataSources.Add("Application!*");
            }

            // Schedule log transfers into storage
            //diagConfig.DiagnosticInfrastructureLogs.ScheduledTransferLogLevelFilter = LogLevel.Error;
            diagConfig.DiagnosticInfrastructureLogs.ScheduledTransferLogLevelFilter = Microsoft.WindowsAzure.Diagnostics.LogLevel.Information;
            diagConfig.DiagnosticInfrastructureLogs.ScheduledTransferPeriod = TimeSpan.FromMinutes(5);

            // Specify whether full crash dumps should be captured 
            Microsoft.WindowsAzure.Diagnostics.CrashDumps.EnableCollection(fullCrashDumps);

            return diagConfig;
        }
        private static void RoleEnvironmentChanging(object sender, RoleEnvironmentChangingEventArgs e)
        {
            int i = 1;
            foreach (var c in e.Changes)
            {
                Trace.WriteLine(string.Format("RoleEnvironmentChanging: #{0} Type={1} Change={2}", i++, c.GetType().FullName, c));
            }

            // If a configuration setting is changing);
            if (e.Changes.Any((RoleEnvironmentChange change) => change is RoleEnvironmentConfigurationSettingChange))
            {
                // Set e.Cancel to true to restart this role instance
                e.Cancel = true;
            }
        }
        private static void SetupEnvironmentChangeHandlers()
        {
            // For information on handling configuration changes see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            #region Setup CloudStorageAccount Configuration Setting Publisher
            // From: http://www.tsjensen.com/blog/2009/11/30/Windows+Azure+10+CloudTableClient+Minimal+Configuration.aspx
            // This code sets up a handler to update CloudStorageAccount instances when their corresponding
            // configuration settings change in the service configuration file.


#if false 
            CloudStorageAccount.SetConfigurationSettingPublisher((string configName, Func<string, bool> configSetter) =>
            {
                // Provide the configSetter with the initial value
                configSetter(RoleEnvironment.GetConfigurationSettingValue(configName));

                RoleEnvironment.Changed += (object sender, RoleEnvironmentChangedEventArgs e) =>
                {
                    if (e.Changes.OfType<RoleEnvironmentConfigurationSettingChange>()
                        .Any((RoleEnvironmentConfigurationSettingChange change) => (change.ConfigurationSettingName == configName)))
                    {
                        // The corresponding configuration setting has changed, propagate the value
                        if (!configSetter(RoleEnvironment.GetConfigurationSettingValue(configName)))
                        {
                            // In this case, the change to the storage account credentials in the
                            // service configuration is significant enough that the role needs to be
                            // recycled in order to use the latest settings. (for example, the 
                            // endpoint has changed)
                            RoleEnvironment.RequestRecycle();
                        }
                    }
                };
            });
#endif
            #endregion
        }
    }
}
