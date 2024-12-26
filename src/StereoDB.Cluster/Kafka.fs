namespace StereoDB.Cluster

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Confluent.Kafka
open Confluent.Kafka.Admin
open FsToolkit.ErrorHandling
open Serilog
open StereoDB.Cluster.Utils

type KafkaSettings = {    
    KafkaBootstrapServers: string
    KafkaHeartbeatTopicPrefix: string
    KafkaHeartbeatTopicRetention: TimeSpan
    HeartbeatInterval: TimeSpan
}
with
    static member Default = {        
        KafkaBootstrapServers = "localhost:19092"
        KafkaHeartbeatTopicPrefix = "stereodb_cluster"
        KafkaHeartbeatTopicRetention = TimeSpan.FromSeconds 10
        HeartbeatInterval = TimeSpan.FromSeconds 5
    }

module internal Kafka =
    
    let createHeartbeatTopicName heartbeatTopicPrefix clusterId =
        $"%s{heartbeatTopicPrefix}-%s{clusterId}"
        
    type KafkaClusterClient(logger: ILogger, cancelToken: CancellationToken, clusterId: string, settings: KafkaSettings) =
        
        let createHeartbeatTopic topicName = task {
            use adminClient =
                AdminClientBuilder(AdminClientConfig(BootstrapServers = settings.KafkaBootstrapServers)).Build()

            try
                do! adminClient.CreateTopicsAsync(
                    [TopicSpecification(
                        Name = topicName,
                        Configs = Dictionary<string, string> (
                            dict [("retention.ms", settings.KafkaHeartbeatTopicRetention.TotalMilliseconds.ToString())]
                        )
                    )],
                    CreateTopicsOptions())
            with
                ex -> logger.Error(ex, "Error during creating heartbeat topic")
        }
        
        let startListenHeartbeats (topicName: string) = task {
            use kafkaConsumer =
                ConsumerBuilder<string, string>(
                    ConsumerConfig(
                        BootstrapServers = settings.KafkaBootstrapServers,
                        AutoOffsetReset = AutoOffsetReset.Latest,
                        GroupId = settings.KafkaHeartbeatTopicPrefix)).Build()

            kafkaConsumer.Subscribe topicName
            
            do! Task.Yield()
            
            while not cancelToken.IsCancellationRequested do
                try
                    let msg = kafkaConsumer.Consume cancelToken
                    logger.Information("Got heartbeat message", msg.Message.Value)
                with
                    ex -> logger.Error(ex, "Error during consuming heartbeat messages")

            kafkaConsumer.Close()
        }
        
        let startHeartbeat () = task {
            do! Task.Yield()
            
            let topicName = createHeartbeatTopicName settings.KafkaHeartbeatTopicPrefix clusterId
            
            do! createHeartbeatTopic topicName
            startListenHeartbeats topicName |> ignore

            use producer =
                ProducerBuilder<string, string>(ProducerConfig(
                    BootstrapServers = settings.KafkaBootstrapServers,
                    Acks = Acks.Leader)
                ).Build()

            let message = Message<string, string>(Value = $"Heartbeat {IPAddress.nodeIp}")
            
            while not cancelToken.IsCancellationRequested do
                try                                        
                    do! producer.ProduceAsync(topicName, message) |> Task.ignore                
                with
                    ex -> logger.Error(ex, "Error during sending heartbeat")
                    
                do! Task.Delay settings.HeartbeatInterval
        }
        
        member this.StartHeartbeat() = startHeartbeat()