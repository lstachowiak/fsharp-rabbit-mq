namespace SimpleMQ

open System
open System.Text
open RabbitMQ.Client.Events

type Trace =
    { routingKey: string
      replyTo: string
      correlationId: Guid
      tracePoints: Guid array }

    static member Empty =
        { routingKey = ""
          replyTo = ""
          correlationId = Guid.Empty
          tracePoints = Array.empty }

    static member ofRmqEvent (rmqEvent: BasicDeliverEventArgs) =
        let _, traceHeader = match rmqEvent.BasicProperties.Headers with
                                | null -> false, null
                                | it -> it.TryGetValue("trace")
        let tracePoints =
            match traceHeader with
            | :? (Collections.Generic.List<obj>) as list ->
                list
                    .ConvertAll(fun it -> Guid.Parse(Encoding.UTF8.GetString(it :?> byte [])))
                    .ToArray()
            | _ -> Array.empty

        let correlationId =
            if isNull rmqEvent.BasicProperties.CorrelationId
            then Guid.Empty
            else Guid.Parse(rmqEvent.BasicProperties.CorrelationId)

        { routingKey = rmqEvent.RoutingKey
          replyTo = rmqEvent.BasicProperties.ReplyTo
          correlationId = correlationId
          tracePoints = tracePoints }

    member internal this.Next() =
        let newTracePoints =
            Array.create (this.tracePoints.Length + 1) (Guid.NewGuid())

        Array.Copy(this.tracePoints, newTracePoints, this.tracePoints.Length)

        { routingKey = this.routingKey
          replyTo = this.replyTo
          correlationId = this.correlationId
          tracePoints = newTracePoints }

type Body = string

type MQConsumer = Body -> Trace -> unit

type MQBindingKey = string

type MQBinding = MQBindingKey * MQConsumer

type SimpleMQ =
    abstract EventQueue: name:string * ?prefetchCount:int * bindings:MQBinding list -> unit
    abstract QueryQueue: name:string * ?prefetchCount:int * bindings:MQBinding list -> unit

    abstract PublishQuery: Trace * routingKey:string * Body * ?contentType:string -> Async<Body>
    abstract PublishEvent: Trace * routingKey:string * Body * ?contentType:string -> Trace
    abstract PublishResponse: Trace * Body * ?contentType:string -> unit

type private BindingKeyPattern =
    | Exact of string
    | StartsWith of string

type internal Binding(bindingKey: MQBindingKey, consumer: MQConsumer) =
    let pattern =
        if bindingKey.EndsWith(".#") then StartsWith(bindingKey.Replace(".#", "")) else Exact bindingKey

    member val Consumer = consumer

    member this.Matches routingKey =
        match pattern with
        | Exact bindingKey -> routingKey = bindingKey
        | StartsWith bindingKey -> routingKey.StartsWith(bindingKey)
