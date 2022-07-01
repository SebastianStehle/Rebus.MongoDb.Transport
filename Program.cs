using MongoDB.Driver;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Handlers;

namespace ConsoleApp2
{
    public class Program
    {
        public static void Main()
        {
            using var activator = new BuiltinHandlerActivator();

            var print = new PrintDateTime();

            activator.Register(() => print);

            var mongoClient = new MongoClient("mongodb://localhost");
            var mongoDatabase = mongoClient.GetDatabase("foo");

            Configure.With(activator)
                .Options(o => {
                    o.SetNumberOfWorkers(5);
                    o.SetMaxParallelism(10);
                })
                .Logging(x => x.Console(Rebus.Logging.LogLevel.Info))
                .Transport(t => t.UseMongoDb(mongoDatabase, new MongoTransportOptions { Prefetch = 100 }))
                .Start();

            while (true)
            {
                // activator.Bus.SendLocal(DateTime.Now).Wait();
            }
            var timer = new System.Timers.Timer();
            timer.Elapsed += delegate { ; };
            timer.Interval = 1000;
            timer.Start();

            Console.WriteLine("Press enter to quit");
            Console.ReadLine();
        }

        public class PrintDateTime : IHandleMessages<DateTime>
        {
            private int count = 0;

            public Task Handle(DateTime currentDateTime)
            {
                if (Interlocked.Increment(ref count) % 50 == 0)
                {
                    Console.WriteLine("The time is {0} with {1} count", currentDateTime, count);
                }

                return Task.CompletedTask;
            }
        }
    }
}