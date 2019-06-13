using Microsoft.Azure.EventHubs.Processor;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;


namespace EventHubs
{
    public class Consumer : IEventProcessor
    {
        private Stopwatch _checkpointStopWatch;
        private volatile int _counter;

        /// <summary>
        /// Established a connection to the Event Hub and acquired a least on one partition
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public Task OpenAsync(PartitionContext context)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"I'm listening to Partition {context.Lease.PartitionId} at position {context.Lease.Offset}");

            _checkpointStopWatch = new Stopwatch();
            _checkpointStopWatch.Start();

            return Task.FromResult<object>(null);
        }

        public async Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Releasing lease on Partition {context.Lease.PartitionId}. Reason: {reason}");

            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
                Console.WriteLine($"Partition {context.Lease.PartitionId}: Downloaded {_counter} events");
            }
        }

        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            foreach (var eventData in messages)
            {
                // 1. Convert the stream of bytes to string
                var data = Encoding.UTF8.GetString(eventData.Body);

                // 2. Deserialize the object
                var singleEventReceived = JsonConvert.DeserializeObject<Device>(data);
                Console.WriteLine($"Message received. Partition {context.Lease.PartitionId}, Data:");
                Console.WriteLine($"Sender: {singleEventReceived.Sender} - IP address: {singleEventReceived.IpAddress} - Is online: {singleEventReceived.IsOnline}");
                _counter++;

                // every 5 minutes we set a checkpoint, in the event that the application failed it restarts from last checkpoint
                if (_checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
                {
                    await context.CheckpointAsync();
                    _checkpointStopWatch.Restart();
                }
            }
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error during process event on Partition {context.Lease.PartitionId}. Error: {error.Message}");

            return Task.FromResult<object>(null);
        }
    }
}
