using RabbitMQ.Client;
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

await channel.ExchangeDeclareAsync("firstexchange", ExchangeType.Direct);
await channel.ExchangeDeclareAsync("secondexchange", ExchangeType.Fanout);

await channel.ExchangeBindAsync("secondexchange", "firstexchange", "routing1");
string message = "Hi hossein, How are you?";
var body = Encoding.UTF8.GetBytes(message);

await Task.Delay(2000);

await channel.BasicPublishAsync("firstexchange", "routing1", body);
//await channel.BasicPublishAsync("firstexchange", "routing2", body);

Console.WriteLine("Message sent");
Console.ReadLine();