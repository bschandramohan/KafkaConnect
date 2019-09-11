package com.bschandramohan.learn.kafka.learnkafka

import java.util.concurrent.TimeUnit

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ConfigurableApplicationContext

@SpringBootApplication
class LearnKafkaApplication

fun main(args: Array<String>) {

    val context = SpringApplication.run(LearnKafkaApplication::class.java, *args)

    runSamples(context)

    context.close()
}

private fun runSamples(context: ConfigurableApplicationContext) {
    val producer = context.getBean(MessageProducer::class.java)
    val listener = context.getBean(MessageListener::class.java)

    /*
     * Sending a Hello World message to topic 'siri'.
     * Must be received by both listeners with group foo
     * and bar with containerFactory fooKafkaListenerContainerFactory
     * and barKafkaListenerContainerFactory respectively.
     * It will also be received by the listener with
     * headersKafkaListenerContainerFactory as container factory
     */
    producer.sendMessage("Hello, World!")
    listener.latch.await(10, TimeUnit.SECONDS)

    /*
     * Sending message to a topic with 5 partition,
     * each message to a different partition. But as per
     * listener configuration, only the messages from
     * partition 0 and 3 will be consumed.
     */
    for (i in 0..4) {
        producer.sendMessageToPartion("Hello To Partitioned Topic!", i)
    }
    listener.partitionLatch.await(10, TimeUnit.SECONDS)

    /*
     * Sending message to 'filtered' topic. As per listener
     * configuration,  all messages with char sequence
     * 'World' will be discarded.
     */
    producer.sendMessageToFiltered("Hello Siri!")
    producer.sendMessageToFiltered("Hello World!")
    listener.filterLatch.await(10, TimeUnit.SECONDS)
}
