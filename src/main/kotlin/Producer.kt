import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import models.User
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*

class Producer {
    fun produce(): KafkaProducer<String, User>{
        val property = Properties()
        property[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        property[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        property[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        property[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = "localhost:8081"
        return KafkaProducer(property)
    }
}