using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

Console.Title = "Consumer Exchange to exchange";
Console.WriteLine("Consumer Exchange to exchange");
Console.WriteLine("-----------------------");
var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync("secondexchange", ExchangeType.Fanout);
await channel.QueueDeclareAsync("my-queue");
await channel.QueueBindAsync("my-queue", "secondexchange", string.Empty);

var consumerEvent = new AsyncEventingBasicConsumer(channel);

consumerEvent.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    Console.WriteLine($"Received message: {message}");

    await channel.BasicAckAsync(ea.DeliveryTag, false);
    //return Task.CompletedTask;
};

await channel.BasicConsumeAsync("my-queue", true, consumerEvent);

Console.WriteLine("Consumer finished");
Console.ReadKey();