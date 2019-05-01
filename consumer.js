const {
    Kafka,
    logLevel
} = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'],
    logLevel: logLevel.ERROR
});

const topic = 'test_kafka'

main();

async function main() {
    const consumer = kafka.consumer({
        groupId: 'my-group'
    })


    await consumer.connect()
    await consumer.subscribe({
        topic,
    })

    consumer.run({
        eachMessage: async ({
            topic,
            partition,
            message
        }) => {
            console.log({
                value: message.value.toString(),
                offset: message.offset,
                topic,
                partition
            });
        },
    });
}
