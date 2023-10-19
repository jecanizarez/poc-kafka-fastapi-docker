from kafka import KafkaConsumer
import asyncio

# global variables
loop = asyncio.get_event_loop()


async def consume():
    # create Kafka consumer instance
    # poll for new messages in the topic and print them to the console
    consumer = KafkaConsumer("test.events", bootstrap_servers='kafka:29092', auto_offset_reset='earliest',
                             enable_auto_commit=False)
    for msg in consumer:
        print(f"Consumed msg: {msg}")


loop.run_until_complete(consume())
