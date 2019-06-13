using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventHubs;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace ConsoleApp
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
                const string eventHubConnectionString = ""; // TODO
                const string eventHubName = ""; // TODO
                const string storageConnectionString = ""; // TODO
                const string storageContainerName = ""; // TODO

                // ### publisher sending messages ###
                var publisher = new Publisher();
                publisher.Init(eventHubConnectionString);
                await SendEvents(publisher, 100, true);
                // ###

                var eventProcessorHost = new EventProcessorHost(eventHubName,
                    PartitionReceiver.DefaultConsumerGroupName,
                    eventHubConnectionString, storageConnectionString, storageContainerName);

                // Registers the Event Processor Host and starts receiving messages
                await eventProcessorHost.RegisterEventProcessorAsync<Consumer>();

                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine("Receiving. Press ENTER to stop worker.");
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
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex);
                Console.ReadLine();
            }
        }

        /// <summary>
        /// Send generate events
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="numberOfEvents"></param>
        /// <param name="inBatch"></param>
        private static async Task SendEvents(Publisher publisher, int numberOfEvents, bool inBatch)
        {
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("Sending messages...");

            var events = GenerateEvents(numberOfEvents);

            if (inBatch)
            {
                // send events in batch
                Console.WriteLine($"{events.Count} messages sending in batch...");
                await publisher.PublishInBatch(events);
            }
            else
            {
                // send event one by one
                Console.WriteLine($"{events.Count} messages sending...");
                var totalEvents = events.Count;
                var count = 0;
                foreach (var @event in events)
                {
                    count++;
                    await publisher.Publish(@event);
                    Console.WriteLine($"{count}/{totalEvents}");
                }
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Messages sent.");
        }

        /// <summary>
        /// Generate random event
        /// </summary>
        /// <param name="numberOfEvents"></param>
        private static List<Device> GenerateEvents(int numberOfEvents)
        {
            if (numberOfEvents <= 0) numberOfEvents = 10;

            var rnd = new Random(DateTime.Now.Millisecond);
            var res = new List<Device>();
            var senderValues = Enum.GetValues(typeof(Sender));

            for (var i = 0; i < rnd.Next(numberOfEvents, numberOfEvents); i++)
            {
                res.Add(new Device()
                {
                    Sender = (Sender)senderValues.GetValue(rnd.Next(senderValues.Length)),
                    IpAddress = $"{rnd.Next(0, 255)}.{rnd.Next(0, 255)}.{rnd.Next(0, 255)}.{rnd.Next(0, 255)}",
                    IsOnline = rnd.Next(0, 1) == 1
                });
            }

            return res;
        }
    }
}
