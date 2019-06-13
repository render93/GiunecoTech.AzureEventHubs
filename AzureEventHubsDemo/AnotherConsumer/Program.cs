using System;
using System.Threading.Tasks;
using EventHubs;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace AnotherConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private static async Task MainAsync()
        {
            try
            {
                Console.WriteLine("Consumer listening...");

                const string eventHubConnectionString = ""; // TODO
                const string eventHubName = ""; // TODO
                const string storageConnectionString = ""; // TODO
                const string storageContainerName = ""; // TODO

                var eventProcessorHost = new EventProcessorHost(eventHubName,
                    PartitionReceiver.DefaultConsumerGroupName,
                    eventHubConnectionString, storageConnectionString, storageContainerName);

                // Registers the Event Processor Host and starts receiving messages
                await eventProcessorHost.RegisterEventProcessorAsync<Consumer>();

                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine("Press ENTER to stop worker.");
                Console.ReadLine();

                // Disposes of the Event Processor Host
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine("Shoutdown... Please wait");
                await eventProcessorHost.UnregisterEventProcessorAsync();

                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine("Stop worker. Press ENTER to close application.");
                Console.ReadLine();

                Environment.Exit(0);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}
