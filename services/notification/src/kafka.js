const { Kafka, logLevel } = require('kafkajs');

module.exports = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  logLevel: logLevel.ERROR,
  retry: { initialRetryTime: 3000, retries: 15 },
});
