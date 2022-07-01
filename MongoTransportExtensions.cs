using MongoDB.Driver;
using Rebus.Config;
using Rebus.Time;
using Rebus.Transport;

namespace ConsoleApp2
{
    public static class MongoTransportExtensions
    {
        public static void UseMongoDb(this StandardConfigurer<ITransport> configurer, IMongoDatabase database, MongoTransportOptions? options = null)
        {
            configurer.Register(x => new MongoTransport(x.Get<IRebusTime>(), database, options ?? new MongoTransportOptions()));
        }
    }
}