from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_topic(topic_name, partitions=3, replication_factor=1):
    """Create a Kafka topic with error handling."""
    
    admin_client = KafkaAdminClient(
        bootstrap_servers='kafka:9092',
        client_id='topic-manager'
    )
    
    topic = NewTopic(
        name=topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor
    )
    
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created with {partitions} partitions")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()

create_topic('packets', partitions=2)
