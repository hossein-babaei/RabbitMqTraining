using RabbitMQ.Client;
using System.Text;

Console.Title = "Producer _ Fanout";
var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

var props = new BasicProperties();

string exchangeName = "hossein_exchange_fanout";
await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Fanout);

bool isRepeat = true;

while (isRepeat)
{
    Console.WriteLine("Please enter command:");
    Console.WriteLine("Hint: 'p' to publish and 'e' to end.");
    string command = Console.ReadLine();

    switch (command)
    {
        case "p":
            Console.WriteLine("Please enter message:");
            string message = Console.ReadLine();

            var body = Encoding.UTF8.GetBytes(message);
            await channel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: "",
                mandatory: false,
                basicProperties: props,
                body: body);

            Console.WriteLine("---------------------------------------------------");

            break;

        default:
        case "e":
            isRepeat = false;
            break;
    }
}

Console.ReadLine();