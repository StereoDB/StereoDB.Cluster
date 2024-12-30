module Tests

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Confluent.Kafka

open Xunit
open Swensen.Unquote

open StereoDB.Cluster

[<Fact>]
let ``JoinCluster should trigger sending heartbeat`` () = task {

    let settings = ClusterSettings.Default
    let kafkaSettings = settings.KafkaSettings
    use node = StereoDbNode.Create(settings)
    node.JoinCluster()
    
    use kafkaConsumer = ConsumerBuilder<string, string>(
        ConsumerConfig(
            BootstrapServers = kafkaSettings.KafkaBootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Latest,
            GroupId = Random.Shared.NextInt64().ToString())).Build()

    use cts = new CancellationTokenSource()
    cts.CancelAfter(TimeSpan.FromSeconds 5) // wait for heartbeat messages to be sent

    let tcs = TaskCompletionSource()
    let heartbeatMessages = ConcurrentBag<string>()

    task {
        let heartbeatTopic =
            Kafka.createHeartbeatTopicName kafkaSettings.KafkaHeartbeatTopicPrefix settings.ClusterId
            
        kafkaConsumer.Subscribe heartbeatTopic
        
        try
            while not cts.IsCancellationRequested do
                let msg = kafkaConsumer.Consume(cts.Token)
                heartbeatMessages.Add(msg.Message.Value)
        with
            _ ->
                kafkaConsumer.Close()
                tcs.SetResult()                
    } |> ignore

    do! tcs.Task

    test <@ heartbeatMessages.Count > 0 @>
}