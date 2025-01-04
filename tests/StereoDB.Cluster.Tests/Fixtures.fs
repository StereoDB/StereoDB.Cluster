module Fixtures

open System

open Confluent.Kafka

open StereoDB.Cluster

module KafkaFixtures =
    type KafkaConsumerFixture (kafkaSettings: KafkaSettings) =
        let kafkaConsumer = ConsumerBuilder<string, string>(
            ConsumerConfig(
                BootstrapServers = kafkaSettings.KafkaBootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Latest,
                GroupId = Random.Shared.NextInt64().ToString())).Build()

        member _.Consumer = kafkaConsumer

        interface IDisposable with
            member _.Dispose (): unit = 
                kafkaConsumer.Close()
                kafkaConsumer.Dispose()


