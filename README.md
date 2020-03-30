# thoth-advise-reporter

This repo contains two jobs:

    PRODUCER: Analyze all adviser runs present on Ceph and create Kafka mesages. (CronJob run every day)

    CONSUMER: Take Kafka messages produced by the producer and push metrics to Pushgateway.
