import producer_server
from CONSTANTS import BOOTSTRAP_SERVER


def run_kafka_server():
    input_file = "police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="police.calls.service",
        bootstrap_servers=BOOTSTRAP_SERVER,
        client_id="producer.calls_for_service",
    )
    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
