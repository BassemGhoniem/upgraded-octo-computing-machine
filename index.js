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

    const producer = kafka.producer()
    await producer.connect()
    for (let i = 0; i < 10; i++) {
        let result = await producer.send({
            topic,
            messages: [{
                value: 'Hello KafkaJS user!' + i
            }, ],
        });
        console.log(result);
    }
    await producer.disconnect();
    await admin.disconnect()
}