using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Messages;
using Rebus.Time;
using Rebus.Transport;
using System.Collections.Concurrent;

namespace ConsoleApp2
{
    public sealed class MongoTransport : AbstractRebusTransport
    {
        private static readonly UpdateDefinitionBuilder<MongoMessage> Update = Builders<MongoMessage>.Update;
        private readonly ConcurrentQueue<MongoMessage> prefetchedMessages = new ConcurrentQueue<MongoMessage>();
        private readonly IMongoCollection<MongoMessage> collection;
        private readonly IRebusTime timer;
        private readonly MongoTransportOptions options;

        public MongoTransport(IRebusTime timer, IMongoDatabase database, string address, MongoTransportOptions options)
            : base(address)
        {
            this.timer = timer;

            collection = database.GetCollection<MongoMessage>("Queue");
            collection.Indexes.CreateMany(
                new[]
                {
                    new CreateIndexModel<MongoMessage>(
                        Builders<MongoMessage>.IndexKeys
                            .Ascending(x => x.IsHandled)
                            .Ascending(x => x.DestinationAddress)
                            .Ascending(x => x.TimeToDefer)),
                    new CreateIndexModel<MongoMessage>(
                        Builders<MongoMessage>.IndexKeys
                            .Ascending(x => x.TimeToLive),
                        new CreateIndexOptions
                        {
                            ExpireAfter = TimeSpan.Zero,
                        })
                });

            this.options = options;
        }

        public override void CreateQueue(string address)
        {
        }

        public override async Task<TransportMessage?> Receive(ITransactionContext context,
            CancellationToken cancellationToken)
        {
            var now = timer.Now.UtcDateTime;

            if (options.Prefetch <= 0)
            {
                var mongoMessage =
                    await collection.FindOneAndUpdateAsync<MongoMessage>(
                        x => !x.IsHandled && x.DestinationAddress == Address && x.TimeToDefer < now,
                        Update.Set(x => x.IsHandled, true),
                        new FindOneAndUpdateOptions<MongoMessage, MongoMessage>
                        {
                            Sort = Builders<MongoMessage>.Sort.Ascending(x => x.TimeToDefer)
                        },
                        cancellationToken);

                if (mongoMessage == null)
                {
                    return null;
                }

                CompleteMessage(context, mongoMessage);

                return mongoMessage.ToTransportMessage();
            }

            if (prefetchedMessages.TryDequeue(out var dequeuedMessage))
            {
                CompleteMessage(context, dequeuedMessage);

                return dequeuedMessage.ToTransportMessage();
            }

            // There is no way to limit the updates, therefore we have to query candidates first.
            var candidates =
                await collection.Find(x => !x.IsHandled && x.DestinationAddress == Address && x.TimeToDefer < now)
                    .Limit(options.Prefetch)
                    .Project<MongoMessageId>(Builders<MongoMessage>.Projection.Include(x => x.Id))
                    .ToListAsync(cancellationToken);
            
            if (candidates.Count == 0)
            {
                return null;
            }

            var ids = candidates.Select(x => x.Id).ToList();

            // We cannot modify many documents at the same time and return them, therefore we try this approach.
            var updateId = Guid.NewGuid().ToString();

            var update = 
                await collection.UpdateManyAsync(x => ids.Contains(x.Id),
                    Update.Set(x => x.IsHandled, true).Set(x => x.PrefetchId, updateId),
                    null,
                    cancellationToken);

            var mongoMessages =
                await collection.Find(x => x.PrefetchId == updateId)
                    .ToListAsync(cancellationToken);

            foreach (var message in mongoMessages)
            {
                prefetchedMessages.Enqueue(message);
            }

            if (prefetchedMessages.TryDequeue(out dequeuedMessage))
            {
                CompleteMessage(context, dequeuedMessage);

                return dequeuedMessage.ToTransportMessage();
            }

            return null;

        }

        private void CompleteMessage(ITransactionContext context, MongoMessage mongoMessage)
        {
            context.OnCompleted(async _ =>
            {
                // Ingore cancellation, better to delete the message even if cancelled.
                await collection.DeleteOneAsync(x => x.Id == mongoMessage.Id);
            });

            context.OnAborted(_ =>
            {
                try
                {
                    collection.UpdateOneAsync(x => x.Id == mongoMessage.Id, Update.Set(x => x.IsHandled, false)).Forget();
                }
                catch
                {
                    // Ignore exceptions here.
                }
            });
        }

        protected override async Task SendOutgoingMessages(IEnumerable<OutgoingMessage> outgoingMessages, ITransactionContext context)
        {
            var request = outgoingMessages.Select(x => new MongoMessage
            {
                Id = Guid.NewGuid().ToString(),
                DestinationAddress = x.DestinationAddress,
                MessageHeaders = x.TransportMessage.Headers,
                MessageBody = x.TransportMessage.Body,
                TimeToLive = GetTimeToLive(x.TransportMessage.Headers),
                TimeToDefer = GetTimeToDefer(x.TransportMessage.Headers)
            }).ToList();

            // InsertManyAsync requires at least one item.
            if (request.Count == 0)
            {
                return;
            }

            try
            {
                await collection.InsertManyAsync(request);
            }
            catch (Exception exception)
            {
                var errorText = $"Could not send messages.";

                throw new RebusApplicationException(exception, errorText);
            }
        }

        private DateTime GetTimeToLive(IReadOnlyDictionary<string, string> headers)
        {
            var time = TimeSpan.FromDays(30);

            if (headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceivedString))
            {
                if (TimeSpan.TryParse(timeToBeReceivedString, out var parsed))
                {
                    time = parsed;
                }
            }

            return timer.Now.UtcDateTime + time;
        }

        private DateTime GetTimeToDefer(IReadOnlyDictionary<string, string> headers)
        {
            var time = TimeSpan.Zero;

            if (headers.TryGetValue(Headers.DeferredUntil, out var timeToBeReceivedString))
            {
                if (TimeSpan.TryParse(timeToBeReceivedString, out var parsed))
                {
                    time = parsed;
                }
            }

            return timer.Now.UtcDateTime + time;
        }

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        private sealed class MongoMessageId
        {
            public string Id { get; init; }
        }

        private sealed class MongoMessage
        {
            public string Id { get; init; }

            [BsonElement("b")]
            public byte[] MessageBody { get; init; }

            [BsonElement("h")]
            public Dictionary<string, string> MessageHeaders { get; init; }

            [BsonElement("a")]
            public string DestinationAddress { get; init; }

            [BsonElement("ttl")]
            public DateTime TimeToLive { get; init; }

            [BsonElement("td")]
            public DateTime TimeToDefer { get; init; }

            [BsonElement("pf")]
            public string PrefetchId { get; set; }

            [BsonElement("p")]
            public bool IsHandled { get; init; }

            public TransportMessage ToTransportMessage()
            {
                return new TransportMessage(MessageHeaders, MessageBody);
            }
        }
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
    }
}