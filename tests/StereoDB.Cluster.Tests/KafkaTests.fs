module Tests

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks

open Xunit
open Swensen.Unquote

open StereoDB.Cluster
open Fixtures.KafkaFixtures

type HeartbeatTests() =
    let settings = ClusterSettings.Default
    let kafkaConsumerFixture = new KafkaConsumerFixture(settings.KafkaSettings)

    interface IDisposable with
        member _.Dispose() =
            (kafkaConsumerFixture :> IDisposable).Dispose()

    [<Fact>]
    member _.``JoinCluster should trigger sending heartbeat`` () = task {
        use node = StereoDbNode.Create(settings)
        node.JoinCluster()

        use cts = new CancellationTokenSource()
        cts.CancelAfter(TimeSpan.FromSeconds 5) // wait for heartbeat messages to be sent

        let tcs = TaskCompletionSource()
        let heartbeatMessages = ConcurrentBag<string>()

        task {
            let heartbeatTopic =
                Kafka.createHeartbeatTopicName settings.KafkaSettings.KafkaHeartbeatTopicPrefix settings.ClusterId
            
            kafkaConsumerFixture.Consumer.Subscribe heartbeatTopic
        
            try
                while not cts.IsCancellationRequested do
                    let msg = kafkaConsumerFixture.Consumer.Consume cts.Token
                    heartbeatMessages.Add(msg.Message.Value)
            with
                _ -> tcs.SetResult()                
        } |> ignore

        do! tcs.Task

        test <@ heartbeatMessages.Count > 0 @>
    }
