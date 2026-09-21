using RabbitMQ.Client;

var connectionFactory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var connection = await connectionFactory.CreateConnectionAsync();
var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(
    queue: "MyQueue", 
    durable: true, 
    exclusive: false,
    autoDelete: false, 
    arguments: null);

string message = "Hello, Hossein from producer!";
var body = System.Text.Encoding.UTF8.GetBytes(message);

var props = new BasicProperties();

await channel.BasicPublishAsync(
    exchange: "", 
    routingKey: "MyQueue",
    mandatory: true, 
    basicProperties: props,
    body: body);

Console.WriteLine("Message sent!");
Console.ReadLine();