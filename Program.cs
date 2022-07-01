using MongoDB.Driver;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Routing.TypeBased;
using Rebus.Subscriptions;

namespace ConsoleApp2
{
    public class Program
    {
        public static void Main()
        {
            using var activator1 = NewMethod(true);
            using var activator2 = NewMethod(false);
            using var activator3 = NewMethod(false);

            Console.WriteLine("Press enter to quit");
            Console.ReadLine();
        }

        private static IDisposable NewMethod(bool send)
        {
            var activator = new BuiltinHandlerActivator();

            var print = new PrintDateTime();

            activator.Register(() => print);

            var mongoClient = new MongoClient("mongodb://localhost");
            var mongoDatabase = mongoClient.GetDatabase("foo");

            var bus = Configure.With(activator)
                .Options(o =>
                {
                    o.SetNumberOfWorkers(5);
                    o.SetMaxParallelism(10);
                })
                .Subscriptions(s =>
                {
                    s.StoreInMongoDb(mongoDatabase, "Subscriptions", true);
                })
                .Logging(x => x.Console(Rebus.Logging.LogLevel.Info))
                .Transport(t => t.UseMongoDb(mongoDatabase, Guid.NewGuid().ToString(), new MongoTransportOptions { Prefetch = 100 }))
                .Start();

            var timer = new System.Timers.Timer();
            timer.Elapsed += delegate {; };
            timer.Interval = 1000;
            timer.Start();

            bus.Subscribe(typeof(DateTime)).Wait();

            Task.Run(async () =>
            {
                while (true && send)
                {
                    try
                    {
                        await bus.Publish(DateTime.UtcNow);

                    }
                    catch
                    {

                    }

                    await Task.Delay(1080);
                }
            });
            
            return bus;
        }

        public class PrintDateTime : IHandleMessages<DateTime>
        {
            private int count = 0;

            public Task Handle(DateTime currentDateTime)
            {
                Console.WriteLine("The time is {0} with {1} count", currentDateTime, count);

                return Task.CompletedTask;
            }
        }
    }
}