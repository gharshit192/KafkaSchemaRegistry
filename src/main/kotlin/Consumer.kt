import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import model.User
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

class Consumer {
    fun consumer(): KafkaConsumer<String,User> {
        val property = Properties()
        property[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        property[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        property[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        property[ConsumerConfig.GROUP_ID_CONFIG] = "Group-1-A"
        property[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = "localhost:8081"
        property[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        property[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"

        val kafkaConsumer = KafkaConsumer<String, User>(property)
        kafkaConsumer.subscribe(listOf("Topic-1-A"))
        return kafkaConsumer
    }
}