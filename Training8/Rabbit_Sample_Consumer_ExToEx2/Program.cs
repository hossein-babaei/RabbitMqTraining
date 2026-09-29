using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

Console.Title = "Consumer Exchange to exchange 2";
Console.WriteLine("Consumer Exchange to exchange 2");
Console.WriteLine("-----------------------");
var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync("firstexchange", ExchangeType.Direct);
await channel.QueueDeclareAsync("my-queue-two");
await channel.QueueBindAsync("my-queue-two", "firstexchange", "routing2", null);

var consumerEvent = new AsyncEventingBasicConsumer(channel);

consumerEvent.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    Console.WriteLine($"Received message: {message}");

    await channel.BasicAckAsync(ea.DeliveryTag, false);
    //return Task.CompletedTask;
};

await channel.BasicConsumeAsync("my-queue-two", true, consumerEvent);

Console.WriteLine("Consumer finished");
Console.ReadKey();