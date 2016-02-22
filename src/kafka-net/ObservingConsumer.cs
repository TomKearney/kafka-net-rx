using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Subjects;
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
    public class ObservingConsumer : ConsumerBase
    {
 
        private readonly Subject<Message> _subject = new Subject<Message>();


        public ObservingConsumer(ConsumerOptions options, params OffsetPosition[] positions) : base(options, positions)
        {
        }

        public IObservable<Message> Consume(CancellationToken? cancellationToken = null)
        {
            Options.Log.DebugFormat("Consumer: Beginning consumption of topic: {0}", Options.Topic);
            EnsurePartitionPollingThreads();
            return _subject;
        }

        protected override void ConsumeMessage(Message message, CancellationToken cancellationToken)
        {
            _subject.OnNext(message);
        }
    }
}
