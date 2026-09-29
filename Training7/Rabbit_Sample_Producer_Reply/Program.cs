using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

Console.Title = "Consumer request";
Console.WriteLine("Consumer request");
Console.WriteLine("-----------------------");
var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync("request-queue", durable: false, exclusive: true, autoDelete: true);

var consumerEvent = new AsyncEventingBasicConsumer(channel);
consumerEvent.ReceivedAsync += async (s, e) =>
{
    Console.WriteLine($"CorrelationId: {e.BasicProperties.CorrelationId}");
    Console.WriteLine($"ReplyTo: {e.BasicProperties.ReplyTo}");

    string message = $"This message is response for {e.BasicProperties.CorrelationId}";
    var body = Encoding.UTF8.GetBytes(message);
    await channel.BasicPublishAsync("", e.BasicProperties.ReplyTo, body);

    await channel.BasicAckAsync(e.DeliveryTag, false);
};

await channel.BasicConsumeAsync("request-queue", true, consumerEvent);

Console.ReadLine();