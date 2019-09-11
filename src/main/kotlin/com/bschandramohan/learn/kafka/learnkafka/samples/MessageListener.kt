package com.bschandramohan.learn.kafka.learnkafka.samples

import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.util.concurrent.CountDownLatch

@Component
class MessageListener {
    val latch = CountDownLatch(3)

    val partitionLatch = CountDownLatch(2)

    val filterLatch = CountDownLatch(2)

    private val greetingLatch = CountDownLatch(1)

    @KafkaListener(topics = ["\${message.topic.name}"], groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
    fun listenGroupFoo(message: String) {
        println("Received Message in group 'foo' by listenGroupFoo: $message")
        latch.countDown()
    }

    // Another listener to the same consumer group
    @KafkaListener(topics = ["\${message.topic.name}"], groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
    fun listenGroupAnother(message: String) {
        println("Received Message in group 'foo' by listenGroupAnother: $message")
        latch.countDown()
    }

    // A different consumer group so that it will not receive the message
    @KafkaListener(topics = ["\${message.topic.name}"], groupId = "bar", containerFactory = "barKafkaListenerContainerFactory")
    fun listenGroupBar(message: String) {
        println("Received Message in group 'bar': $message")
        latch.countDown()
    }

    @KafkaListener(topics = ["\${message.topic.name}"], containerFactory = "headersKafkaListenerContainerFactory")
    fun listenWithHeaders(@Payload message: String, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int) {
        println("Received Message: $message from partition: $partition")
        latch.countDown()
    }

    @KafkaListener(topicPartitions = [TopicPartition(topic = "\${partitioned.topic.name}", partitions = ["0", "3"])])
    fun listenToPartition(@Payload message: String, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int) {
        println("Received Message: $message from partition: $partition")
        partitionLatch.countDown()
    }

    @KafkaListener(topics = ["\${filtered.topic.name}"], containerFactory = "filterKafkaListenerContainerFactory")
    fun listenWithFilter(message: String) {
        println("Received Message in filtered listener: $message")
        filterLatch.countDown()
    }
}
