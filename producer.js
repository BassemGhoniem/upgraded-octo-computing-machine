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

const admin = kafka.admin()

main();

async function main() {
    await admin.connect()
    await admin.createTopics({
        topics: [{
            topic,
            numPartitions: 3
        }]
    });

    const producer = kafka.producer();
    await producer.connect()

    let i = 0;
    setInterval( () => producer.send({
        topic,
        messages: [{
            value: 'Hello KafkaJS user!' + i++,
        }, ],
    }), 1000 * 3);

    // await producer.disconnect();
    await admin.disconnect();
}
