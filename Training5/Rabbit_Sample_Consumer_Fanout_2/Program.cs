using RabbitMQ.Client;
using RabbitMQ.Client.Events;

Console.Title = "Consumer two _ Fanout";
Console.WriteLine("Consumer two");
Console.WriteLine("-----------------------");
var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

string exchangeName = "hossein_exchange_fanout";
await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Fanout);

string queueName = (await channel.QueueDeclareAsync()).QueueName;

await channel.QueueBindAsync(queueName, exchangeName, "");

var consumerEvent = new AsyncEventingBasicConsumer(channel);

consumerEvent.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = System.Text.Encoding.UTF8.GetString(body);
    Console.WriteLine($"Received message: {message}");

    await channel.BasicAckAsync(ea.DeliveryTag, false);
    //return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumerEvent);

Console.WriteLine("Consumer finished");
Console.ReadKey();