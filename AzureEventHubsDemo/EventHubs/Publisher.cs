using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;

namespace EventHubs
{
    public class Publisher
    {
        private EventHubClient _eventHubClient;

        /// <summary>
        /// create new instance of Event Hub Client from connection string
        /// </summary>
        /// <param name="connectionString"></param>
        public void Init(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            _eventHubClient = EventHubClient.CreateFromConnectionString(connectionString);
        }

        /// <summary>
        /// Publish event to hub
        /// </summary>
        /// <param name="myEvent"></param>
        public async Task Publish<T>(T myEvent)
        {
            try
            {
                if (myEvent == null)
                    throw new ArgumentNullException(nameof(myEvent));

                // 1. Serialize the event
                var serializedEvent = JsonConvert.SerializeObject(myEvent);

                // 2. Convert serialized event to byte array
                var eventBytes = Encoding.UTF8.GetBytes(serializedEvent);

                // 3. Wrap event bytes in EventData instance
                var eventData = new EventData(eventBytes);

                // 4. Publish the event
                await _eventHubClient.SendAsync(eventData);
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error!");
                Console.WriteLine(ex);
            }
        }


        /// <summary>
        /// Publish events in batch
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="myEvents"></param>
        public async Task PublishInBatch<T>(IEnumerable<T> myEvents)
        {
            try
            {
                if (myEvents == null)
                    throw new ArgumentNullException(nameof(myEvents));

                var batchData = _eventHubClient.CreateBatch();

                foreach (var myEvent in myEvents)
                {
                    // 1. Serialize the event
                    var serializedEvent = JsonConvert.SerializeObject(myEvent);

                    // 2. Convert serialized event to byte array
                    var eventBytes = Encoding.UTF8.GetBytes(serializedEvent);

                    // 3. Add event in Batch object
                    var res = batchData.TryAdd(new EventData(eventBytes));
                    if (!res)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Error!");
                        Console.WriteLine("Batch's size limit exceeded. It will be send the previous events added");
                        break;
                    }
                }

                // 4. Publish the events
                await _eventHubClient.SendAsync(batchData);

            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error!");
                Console.WriteLine(ex);
            }
        }
    }
}
