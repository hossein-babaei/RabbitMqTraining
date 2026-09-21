using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(
    queue: "MyQueue",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

var consumerEvent = new AsyncEventingBasicConsumer(channel);

consumerEvent.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = System.Text.Encoding.UTF8.GetString(body);

    await channel.BasicAckAsync(ea.DeliveryTag, false);
    Console.WriteLine($"Received message: {message}");
};

await channel.BasicConsumeAsync("MyQueue", autoAck: false, consumer: consumerEvent);

Console.WriteLine("Consumer finished");
Console.ReadKey();