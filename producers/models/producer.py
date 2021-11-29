"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

# Global variable for this producer instance
BOOTSTRAP_SERVERS = "PLAINTEXT://kafka0:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # Done: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Done: Configure the AvroProducer
        self.producer = AvroProducer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "schema.registry.url": SCHEMA_REGISTRY_URL,
            },
            default_key_schema=key_schema,
            default_value_schema=value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # Done: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
        if self._topic_exists(client=client):
            logger.info(f"Topic {self.topic_name} already exists")
            return
        
        new_topic = NewTopic(
            topic=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas,
            config={
                "cleanup.policy": "delete",
                "compression.type": "lz4",
                "delete.retention.ms": "2000",
                "file.delete.delay.ms": "2000",
            },
        )
        
        futures = client.create_topics([new_topic])
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"topic {self.topic_name} was created successfully")
            except Exception as e:
                logger.error(f"failed to create topic {self.topic_name}: {e}")
                raise

    def _topic_exists(self, client):
        """Checks if the topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        topics = set(t.topic for t in iter(topic_metadata.topics.values()))
        return self.topic_name in topics
    
    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # Done: Write cleanup code for the Producer here
        #
        #
        if self.producer is not None:
            self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
