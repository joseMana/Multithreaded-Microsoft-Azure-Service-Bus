using Azure.Messaging.ServiceBus;
using Microsoft.Azure;
using Microsoft.WindowsAzure;
using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace josephm.SBProcessor
{
    class Program
    {
        private static readonly string _connectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];
        private static readonly string _queueName = ConfigurationManager.AppSettings["Microsoft.ServiceBus.QueueName"];

        readonly static int _numberOfThreads = 8;
        private static Thread[] _queueThreads;
        private object _lock = new object();
        private static ManualResetEvent CompletedEvent = new ManualResetEvent(false);
        static void Main(string[] args)
        {

            int countdown = 5;

            while (countdown > 0)
            {
                Console.Clear();
                Console.WriteLine($"\rStarting SB Processor in ...{countdown--}");
                Thread.Sleep(1000); 
            }

            RunService();
            CompletedEvent.WaitOne();
        }
        static void RunService()
        {
            CreateMultiThreadedReceiveMessagesDelegates();
        }
        static void CreateMultiThreadedReceiveMessagesDelegates()
        {
            _queueThreads = new Thread[_numberOfThreads];
            for (int i = 0; i < _numberOfThreads; i++)
            {
                _queueThreads[i] = new Thread(async () =>
                {
                    await ReceiveMessagesAsync();
                });

                _queueThreads[i].Start();
            }
        }
        static async Task ReceiveMessagesAsync()
        {
            ServiceBusClient client = new ServiceBusClient(_connectionString);
            ServiceBusProcessor processor = client.CreateProcessor(_queueName, new ServiceBusProcessorOptions());
            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;
            await processor.StartProcessingAsync();
        }
        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");

            await args.CompleteMessageAsync(args.Message);
        }
        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Trace.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
    }
}
