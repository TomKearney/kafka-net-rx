using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// Provides a basic consumer of one Topic across all partitions or over a given whitelist of partitions.
    /// 
    /// TODO: provide automatic offset saving when the feature is available in 0.8.2
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class BlockingConsumer : ConsumerBase
    {
        private readonly BlockingCollection<Message> _fetchResponseQueue;
        
        public BlockingConsumer(ConsumerOptions options, params OffsetPosition[] positions) : base(options, positions)
        {
            _fetchResponseQueue = new BlockingCollection<Message>(options.ConsumerBufferSize);
        }

        /// <summary>
        /// Returns a blocking enumerable of messages received from Kafka.
        /// </summary>
        /// <returns>Blocking enumberable of messages from Kafka.</returns>
        public IEnumerable<Message> Consume(CancellationToken? cancellationToken = null)
        {
            Options.Log.DebugFormat("Consumer: Beginning consumption of topic: {0}", Options.Topic);
            EnsurePartitionPollingThreads();
            return _fetchResponseQueue.GetConsumingEnumerable(cancellationToken ?? CancellationToken.None);
        }

        protected override void ConsumeMessage(Message message, CancellationToken cancellationToken)
        {
            _fetchResponseQueue.Add(message, cancellationToken);
        }
    }
}
