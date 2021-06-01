namespace SimpleMQ

open RabbitMQ.Client
open RabbitMQ.Client.Events
open System
open System.Text
open System.Threading
open Utils

type private QueueProps =
    { name: string
      exchangeName: string
      prefetchCount: int
      durable: bool
      exclusive: bool
      autoDelete: bool
      autoAck: bool }

    static member Query moduleName name prefetchCount =
        { name =
              moduleName
              + (if name = "" then "" else ":")
              + name
          exchangeName = "amq.topic"
          prefetchCount = Option.defaultValue 10 prefetchCount
          durable = false
          exclusive = true
          autoDelete = true
          autoAck = true }

    static member Event moduleName name prefetchCount =
        { name =
              moduleName
              + (if name = "" then "" else ":")
              + name
          exchangeName = "amq.topic"
          prefetchCount = Option.defaultValue 1 prefetchCount
          durable = true
          exclusive = false
          autoDelete = false
          autoAck = false }

type private RabbitSimpleMQ(moduleName: string, cf: ConnectionFactory) =

    [<Literal>]
    let NACK_TIMEOUT = 3000

    let conn = cf.CreateConnection()
    let publishChannel = conn.CreateModel()

    let publish exchange routingKey props (body: string) =
        let action () =
            publishChannel.BasicPublish(exchange, routingKey, props, ReadOnlyMemory(Encoding.UTF8.GetBytes body))

        lock publishChannel action

    let createPublishProps (trace: Trace) (contentType: string option) =
        let props = publishChannel.CreateBasicProperties()

        props.Headers <-
            Map.ofList [ "trace",
                         trace.tracePoints
                         |> Array.map (fun t -> t.ToString()) :> obj
                         "publisher", moduleName :> obj
                         "ts", DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ") :> obj ]

        props.ContentType <- Option.defaultValue "application/json" contentType
        props

    let onEvent (p: QueueProps) (bindings: Binding list) (receivingChannel: IModel) (event: BasicDeliverEventArgs) =
        let routingKey = event.RoutingKey
        let trace = Trace.ofRmqEvent event

        let body =
            Encoding.UTF8.GetString(event.Body.ToArray())

        if not (routingKey.StartsWith("query"))
        then logInfo "%s at queue: %s, message:\n%s\n" routingKey p.name body

        try
            bindings
            |> List.tryFind (fun b -> b.Matches routingKey)
            |> Option.iter (fun b -> b.Consumer body trace)

            if not p.autoAck
            then receivingChannel.BasicAck(deliveryTag = event.DeliveryTag, multiple = false)
        with ex ->
            logInfo "ERROR: %A" ex

            if not p.autoAck then
                Thread.Sleep(NACK_TIMEOUT)
                receivingChannel.BasicNack(deliveryTag = event.DeliveryTag, multiple = false, requeue = true)

        ()

    let queue (p: QueueProps) (bindings: MQBinding list) =
        logInfo "MQ: declare queue [%s]" p.name
        let receivingChannel = conn.CreateModel()
        receivingChannel.BasicQos(prefetchSize = 0u, prefetchCount = uint16 p.prefetchCount, ``global`` = true)

        receivingChannel.QueueDeclare
            (queue = p.name, durable = p.durable, exclusive = p.exclusive, autoDelete = p.autoDelete)
        |> ignore

        let bindQueue (key, _) =
            logInfo "MQ: binding queue [%s] to <%s>" p.name key
            receivingChannel.QueueBind(p.name, p.exchangeName, key)

        List.iter bindQueue bindings

        let eventConsumer = EventingBasicConsumer(receivingChannel)
        eventConsumer.Received.Add(onEvent p (List.map Binding bindings) receivingChannel)
        eventConsumer.Shutdown.Add Console.WriteLine

        receivingChannel.BasicConsume(queue = p.name, autoAck = p.autoAck, consumer = eventConsumer)
        |> ignore

        logInfo "MQ: binding queue [%s] done" p.name

    member this.registerPing =
        let queue = moduleName
        let routingKey = "ping"
        logInfo "MQ: binding queue [%s] to <%s>" queue routingKey
        let ch = conn.CreateModel()
        ch.BasicQos(prefetchSize = 0u, prefetchCount = 1us, ``global`` = true)
        ch.QueueDeclare(queue = queue, durable = false, exclusive = true, autoDelete = true)
        |> ignore
        ch.QueueBind(queue, "amq.topic", routingKey)

        let consumer = EventingBasicConsumer(ch)

        let consume (event: BasicDeliverEventArgs) =
            let trace = Trace.ofRmqEvent event

            let props = createPublishProps trace (Some "text/plain")
            props.ReplyTo <- moduleName
            props.CorrelationId <- trace.correlationId.ToString()
            props.Persistent <- false
            publish "" trace.replyTo props moduleName

        consumer.Received.Add consume

        ch.BasicConsume(queue = queue, autoAck = true, consumer = consumer)
        |> ignore

    member this.registerQueryHandler =
        let replyQueueName = "amq.rabbitmq.reply-to"
        let consumer = EventingBasicConsumer(publishChannel)

        let consume (rmqEvent: BasicDeliverEventArgs) =
            let body =
                Encoding.UTF8.GetString(rmqEvent.Body.ToArray())

            let trace = Trace.ofRmqEvent rmqEvent

            this.Queries
            |> DictUtil.tryRemove trace.correlationId
            |> Option.map (fun resolve -> resolve body)
            |> Option.defaultValue ()

        consumer.Received.Add consume

        publishChannel.BasicConsume(queue = replyQueueName, autoAck = true, consumer = consumer)
        |> ignore

    member val Queries = Collections.Concurrent.ConcurrentDictionary<Guid, Body -> unit>()

    interface SimpleMQ with

        member this.QueryQueue(name, prefetchCount, bindings) =
            queue (QueueProps.Query moduleName name prefetchCount) bindings

        member this.EventQueue(name, prefetchCount, bindings) =
            queue (QueueProps.Event moduleName name prefetchCount) bindings

        member this.PublishEvent(oldTrace, routingKey, body, contentType) =
            let trace = oldTrace.Next()
            let props = createPublishProps trace contentType
            props.Persistent <- true
            publish "amq.topic" routingKey props body
            trace

        member this.PublishResponse(oldTrace, body, contentType) =
            let trace = oldTrace.Next()
            let props = createPublishProps trace contentType
            props.Persistent <- false
            props.CorrelationId <- trace.correlationId.ToString()
            publish "" trace.replyTo props body

        member this.PublishQuery(oldTrace, routingKey, body, contentType) =
            let trace = oldTrace.Next()
            let correlationId = Guid.NewGuid()
            let props = createPublishProps trace contentType
            props.ReplyTo <- "amq.rabbitmq.reply-to"
            props.CorrelationId <- correlationId.ToString()
            props.Persistent <- false
            publish "amq.topic" routingKey props body

            Async.FromContinuations(fun (resolve, reject, _) ->
                let timeout =
                    async {
                        do! Async.Sleep 3000
                        this.Queries.TryRemove correlationId |> ignore
                        reject (TimeoutException(sprintf "routing key: %s query body: %O" routingKey body)) // TODO
                    }

                let cts = new CancellationTokenSource()

                let resolveAndCancelTimeout result =
                    cts.Cancel()
                    resolve result

                Async.Start(timeout, cts.Token)

                this.Queries.TryAdd(correlationId, resolveAndCancelTimeout)
                |> ignore)

module RabbitSimpleMQ =

    let connect (moduleName: string) (cf: RabbitMQ.Client.ConnectionFactory) =
        let rmq = RabbitSimpleMQ(moduleName, cf)
        let mq = rmq :> SimpleMQ
        rmq.registerPing
        rmq.registerQueryHandler

        mq
