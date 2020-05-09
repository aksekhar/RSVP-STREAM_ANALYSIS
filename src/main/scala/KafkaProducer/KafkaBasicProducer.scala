package KafkaProducer
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaBasicProducer extends App{
  val props: Properties= new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val prod = new KafkaProducer[String,String](props)
  val test_topic = "test1"
  try {
    for (i <-0 to 5 ) {
      val record = new ProducerRecord[String,String](test_topic,i.toString,"message "+i)
      val metadata = prod.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(),record.value(),metadata.get().partition(),
        metadata.get().offset())
    }
  }catch {
    case e:Exception =>e.printStackTrace()
  }finally {
    prod.close()
  }


}