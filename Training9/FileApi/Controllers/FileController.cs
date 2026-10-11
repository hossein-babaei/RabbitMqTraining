using Microsoft.AspNetCore.Mvc;
using RabbitMQ.Client;
using System.Text;

namespace FileApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class FileController : ControllerBase
    {
        [HttpGet]
        public async Task<IActionResult> Get(string fileName, string fileExtension)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                return BadRequest("File Name is null");
            }

            if (string.IsNullOrEmpty(fileExtension))
            {
                return BadRequest("File Extension is null");
            }

            var connectionFactory = new ConnectionFactory()
            {
                HostName = "localhost"
            };

            using var connection = await connectionFactory.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();

            var props = new BasicProperties();

            string queueName;
            string routingKey;

            switch (fileExtension)
            {
                case ".pdf":
                    queueName = "pdf_queue";
                    routingKey = "pdfroute";
                    break;
                case ".docx":
                    queueName = "docx_queue";
                    routingKey = "docxroute";
                    break;
                case ".xlsx":
                    queueName = "xlsx_queue";
                    routingKey = "xlsxroute";
                    break;
                default:
                    queueName = "";
                    routingKey = "";
                    break;
            }

            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            string exchangeName = "file_api_exchange";
            await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct);

            await channel.QueueBindAsync(queueName, exchangeName, routingKey);

            var body = Encoding.UTF8.GetBytes(fileName);

            await channel.BasicPublishAsync(exchangeName, routingKey, body);


            return Ok("Job done");
        }
    }
}
