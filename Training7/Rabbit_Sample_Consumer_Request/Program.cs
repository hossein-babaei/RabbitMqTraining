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

var replyQueue = await channel.QueueDeclareAsync("", durable: false, exclusive: true, autoDelete: true);

await channel.QueueDeclareAsync("request-queue", durable: false, exclusive: false, autoDelete: true);

var consumerEvent = new AsyncEventingBasicConsumer(channel);

consumerEvent.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    Console.WriteLine($"Received message: {message}");

    await channel.BasicAckAsync(ea.DeliveryTag, false);
    //return Task.CompletedTask;
};

await channel.BasicConsumeAsync(replyQueue.QueueName, autoAck: true, consumer: consumerEvent);

var props = new BasicProperties()
{
    ReplyTo = replyQueue.QueueName,
    CorrelationId = Guid.NewGuid().ToString()
};

var binaryMessage = Encoding.UTF8.GetBytes("this message sent from consumer");
await channel.BasicPublishAsync("", "request-queue", true, props, binaryMessage);

Console.WriteLine("Consumer finished");
Console.ReadKey();