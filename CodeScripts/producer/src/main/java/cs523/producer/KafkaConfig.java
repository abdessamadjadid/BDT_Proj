package cs523.producer;

public class KafkaConfig {

	// Set the Port
    public static final String BOOTSTRAPSERVERS  = "127.0.0.1:9092";
    
    //Set the topic
    public static final String TOPIC = "food";
    
    // Deserialization of the String msg
    public static final String BOOTSTRAP_SERVERS_CONFIG = "StringDeserializer";
    
    //Deserialization of the value 
    public static final String  VALUE_DESERIALIZER_CLASS_CONFIG = "StringDeserializer";

}
