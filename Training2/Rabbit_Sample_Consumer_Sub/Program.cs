using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

string exchangeName = "hossein_exchange_direct";
await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct);

string routingKey = "hossein.routing.key";

var queueDeclareResult = await channel.QueueDeclareAsync();
string queueName = queueDeclareResult.QueueName;

await channel.QueueBindAsync(queueName, exchangeName, routingKey);

var consumerEvent = new AsyncEventingBasicConsumer(channel);

consumerEvent.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = System.Text.Encoding.UTF8.GetString(body);

    //await channel.BasicAckAsync(ea.DeliveryTag, false);
    Console.WriteLine($"Received message 2: {message}");
    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumerEvent);

Console.WriteLine("Consumer 2 finished");
Console.ReadLine();