using RabbitMQ.Client;
using System.Text;

var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

Console.WriteLine("...");
await Task.Delay(2000);
Console.WriteLine("...");

using var connection = await connectionFactory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

var props = new BasicProperties();

string exchangeName = "hossein_exchange_direct";
await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct);
string routingKey = "hossein.routing.key";


string message = "Hello 2, Hossein from producer!";
var body = Encoding.UTF8.GetBytes(message);

await channel.BasicPublishAsync(
    exchange: exchangeName,
    routingKey: routingKey,
    mandatory: false,
    basicProperties: props,
    body: body);

Console.WriteLine($"{exchangeName} with {routingKey} message sent!");
Console.ReadLine();