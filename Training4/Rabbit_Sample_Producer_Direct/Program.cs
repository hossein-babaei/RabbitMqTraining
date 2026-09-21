using RabbitMQ.Client;
using System.Text;

Console.Title = "Producer _ Direct";
var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

string queueName = "my_direct_queue";
string exchangeName = "my_direct_exchange";

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);
await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct);

string routingKey = "mydirectroutingkey";

await channel.QueueBindAsync(queueName, exchangeName, routingKey);

string message = "Hi hossein, how is going on?";
var body = Encoding.UTF8.GetBytes(message);

await channel.BasicPublishAsync(exchangeName, routingKey, body);


Console.WriteLine($"{exchangeName} with {routingKey} message sent!");
Console.ReadLine();
