const amqlib = require('amqplib')

class RabbitMQ {
    constructor() {
        //amqp://guest:guest@q-rabbitmq:5672/
        this.url = `amqp://${process.env.RABBITMQ_LOGIN}:${process.env.RABBITMQ_PASSWORD}@${process.env.RABBITMQ_HOST}:${process.env.RABBITMQ_PORT}/${process.env.RABBITMQ_VHOST}`
        this.connection = null
        this.channel = null
    }

    async connect() {
        //Conecte-se a um servidor 
        if (!this.connection) this.connection = await amqlib.connect(this.url)
        //Cria um canal
        if (!this.channel) this.channel = await this.connection.createChannel()
        //Defina a contagem de pré-busca para este canal. 1 é o número máximo de mensagens enviadas pelo canal que podem estar aguardando confirmação; assim que houver 1 mensagem pendentes, o servidor não enviará mais mensagens neste canal até que uma ou mais tenham sido confirmadas
        this.channel.prefetch(1)
    }

    async send(queue, message) {
        //envia a mensagem do canal
        try {
            if (this.channel) {
                this.channel.assertQueue(queue, { durable: true, queueMode: 'lazy' })
                this.channel.sendToQueue(queue, Buffer.from(JSON.stringify(message), 'utf-8'), { persistent: true })
            }
        }
        catch (error) {
            console.log(error.message)
        }
    }

    async consume(queue, callback) {
        //recebe a mensagem do canal
        try {
            if (this.channel) {
                this.channel.assertQueue(queue, { durable: true, queueMode: 'lazy' })
                this.channel.consume(queue, callback, { noAck: true })
            }
        }
        catch (error) {
            console.log(error.message)
        }
    }
}

class RabbitMQService {
    //realiza a criação, a conexao ao rabbit 
    static async getInstance() {
        if (!RabbitMQService.instance) {
            let instance = new RabbitMQ()
            await instance.connect()
            RabbitMQService.instance = instance
        }
        return RabbitMQService.instance
    }

}

module.exports = RabbitMQService