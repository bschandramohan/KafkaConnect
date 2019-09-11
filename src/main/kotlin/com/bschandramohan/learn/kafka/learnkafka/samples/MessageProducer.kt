package com.bschandramohan.learn.kafka.learnkafka.samples

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import org.springframework.util.concurrent.ListenableFutureCallback

@Component
class MessageProducer {

    @Autowired
    private val kafkaTemplate: KafkaTemplate<String, String>? = null

    @Value(value = "\${message.topic.name}")
    private lateinit var topicName: String

    @Value(value = "\${partitioned.topic.name}")
    private lateinit var partionedTopicName: String

    @Value(value = "\${filtered.topic.name}")
    private lateinit var filteredTopicName: String

    @Value(value = "\${greeting.topic.name}")
    private lateinit var greetingTopicName: String

    fun sendMessage(message: String) {

        val future = kafkaTemplate!!.send(topicName!!, message)

        future.addCallback(object : ListenableFutureCallback<SendResult<String, String>> {

            override fun onSuccess(result: SendResult<String, String>?) {
                println("Sent message=[" + message + "] with offset=[" + result!!.recordMetadata.offset() + "]")
            }

            override fun onFailure(ex: Throwable) {
                println("Unable to send message=[" + message + "] due to : " + ex.message)
            }
        })
    }

    fun sendMessageToPartion(message: String, partition: Int) {
        kafkaTemplate!!.send(partionedTopicName!!, partition, "null", message)
    }

    fun sendMessageToFiltered(message: String) {
        kafkaTemplate!!.send(filteredTopicName!!, message)
    }
}