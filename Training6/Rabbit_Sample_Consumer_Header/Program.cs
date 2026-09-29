using RabbitMQ.Client;
using RabbitMQ.Client.Events;

Console.Title = "Consumer one _ Headers";
Console.WriteLine("Consumer one");
Console.WriteLine("-----------------------");
var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

string exchangeName = "hossein_exchange_headers";
await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Headers);

string queueName = (await channel.QueueDeclareAsync()).QueueName;

Dictionary<string, object?> arguments = new()
{
    {"x-match", "any" }, // any or all | any means one of this args match - all means all of this args must match
    {"name", "hossein" },
    {"age", "31" }
};
await channel.QueueBindAsync(queueName, exchangeName, "", arguments);

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