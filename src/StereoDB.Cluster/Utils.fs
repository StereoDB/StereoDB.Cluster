module internal StereoDB.Cluster.Utils

open System.Net
open System.Net.Sockets

module IPAddress =

    let nodeIp = 
        let ipHostInfo = Dns.GetHostEntry(Dns.GetHostName())
        ipHostInfo.AddressList |> Array.find(fun x -> x.AddressFamily = AddressFamily.InterNetwork)
