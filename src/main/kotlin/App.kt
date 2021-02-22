import model.User
import org.apache.kafka.clients.producer.ProducerRecord
import java.lang.Exception
import java.util.*

fun runProducer(){
        val producer = Producer().produce()
        println("\n Producer Running.... \n")
        try {
        for (i in 0 until 50){
                val user = User("Harsh$i", 23,"CL${i+14}PG${i+10}")
                val record: ProducerRecord<String, User> =
                        ProducerRecord<String, User>("Topic-1-A", "$i", user)
                producer.send(record)
                println("Produced $i")
        }
        }catch (e : Exception){
                print(e.stackTrace)
        }
        println("\n Producer Finished.... \n")
}
fun runConsumer(){
        val consumer = Consumer().consumer()
        println("\n Consumer Running.... \n")
        try {
        consumer.subscribe(listOf("Topic-1-A"))
        println("\n Subscribed to Topic-1-A \n")
        while (true) {
                val consumeRecords = consumer.poll(100)
                for (record in consumeRecords) {
                        println("Key : ${record.key()} , Offset : ${record.offset()} , Value : ${record.value()}")
                        println("Partition Consumed ${record.partition()}")
                }
        }
        }catch (e : Exception){
                println(e.stackTrace)
        }
        println("\n Consumer Finished.... \n")
}
fun main() {
        val inputKey = Scanner(System.`in`)
        print("Enter 1 for Producer and 2 for Consumer : ")
        when (inputKey.nextInt()) {
                1 -> runProducer()
                2 -> runConsumer()
        }
}