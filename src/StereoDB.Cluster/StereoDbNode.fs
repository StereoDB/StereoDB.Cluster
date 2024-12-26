namespace StereoDB.Cluster

open System
open System.Threading
open System.Threading.Tasks
open IcedTasks
open Serilog
open StereoDB.Cluster.Kafka

type ClusterSettings = {
    ClusterId: string
    KafkaSettings: KafkaSettings
}
with
    static member Default = {
        ClusterId = "default_cluster_id"
        KafkaSettings = KafkaSettings.Default
    }

type StereoDbNode(logger, settings) =
    
    let _cancelToken = new CancellationTokenSource()
    let _kafkaClient = KafkaClusterClient(logger, _cancelToken.Token, settings.ClusterId, settings.KafkaSettings)
    let mutable _heartBeatTask = Task.CompletedTask
    
    member this.JoinCluster() =
        _heartBeatTask <- _kafkaClient.StartHeartbeat()
        
    static member Create(settings) =
        let logger = LoggerConfiguration().CreateLogger()
        StereoDbNode(logger, settings)
        
    interface IAsyncDisposable with
        member this.DisposeAsync() =
            valueTask {
                _cancelToken.Cancel()
                do! _heartBeatTask            
            }
            |> ValueTask.toUnit
            
                