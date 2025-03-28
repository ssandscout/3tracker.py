# Tracker Module: Handles position messages and query processing for contact tracing.
import pika
import json
import time
from collections import defaultdict

# Queue names and exchange for messaging
QUERY_TOPIC = 'query'
CONFIG_REQUEST_QUEUE = 'config_request'
CONFIG_RESPONSE_QUEUE = 'config_response'
EXCHANGE_NAME = 'positions'  # Exchange for position updates

class Tracker:
    def __init__(self, host, board_size):
        # Connect to RabbitMQ
        connection_params = pika.ConnectionParameters(host=host)
        self.connection = pika.BlockingConnection(connection_params)
        self.channel = self.connection.channel()

        # Declare exchange and bind a queue to receive position messages
        self.channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')
        result = self.channel.queue_declare(queue="", exclusive=False)
        self.position_queue = result.method.queue
        self.channel.queue_bind(exchange=EXCHANGE_NAME, queue=self.position_queue)

        # Declare queues for query and config messages
        self.channel.queue_declare(queue=QUERY_TOPIC)
        self.channel.queue_declare(queue=CONFIG_REQUEST_QUEUE)
        self.channel.queue_declare(queue=CONFIG_RESPONSE_QUEUE)

        # Initialize storage for positions and contact events
        self.positions = {}  # Maps person_id -> (x, y)
        self.contacts = defaultdict(list)  # Maps person_id -> list of (other_id, timestamp)
        self.board_size = board_size

    def start(self):
        # Start consuming position, query, and config messages
        self.channel.basic_consume(
            queue=self.position_queue,
            on_message_callback=self.on_position_message,
            auto_ack=True
        )
        self.channel.basic_consume(
            queue=QUERY_TOPIC,
            on_message_callback=self.on_query_message,
            auto_ack=True
        )
        self.channel.basic_consume(
            queue=CONFIG_REQUEST_QUEUE,
            on_message_callback=self.on_config_request,
            auto_ack=True
        )
        print("[Tracker] Started. Waiting for messages...")
        self.channel.start_consuming()

    def on_position_message(self, ch, method, properties, body):
        """
        Process a position message.
        Expected JSON:
          {"person_id": <str>, "x": <int>, "y": <int>, "timestamp": <float>}
        """
        try:
            msg = json.loads(body)
            person_id = msg["person_id"]
            x = msg["x"]
            y = msg["y"]
            ts = msg["timestamp"]
        except (json.JSONDecodeError, KeyError):
            print("[Tracker] Invalid 'position' message")
            return

        # Update the position for this person
        self.positions[person_id] = (x, y)

        # Record a contact if another person is at the same cell
        for other_id, (ox, oy) in self.positions.items():
            if other_id != person_id and ox == x and oy == y:
                self.contacts[person_id].append((other_id, ts))
                self.contacts[other_id].append((person_id, ts))

    def on_query_message(self, ch, method, properties, body):
        """
        Process a query message.
        Expected JSON:
          {"person_id": <str>}
        Sends a response with the contact list and current position.
        """
        try:
            msg = json.loads(body)
            person_id = msg["person_id"]
        except (json.JSONDecodeError, KeyError):
            print("[Tracker] Invalid 'query' message")
            return

        # Get sorted contacts and current position
        contact_list = self.contacts.get(person_id, [])
        sorted_contacts = sorted(contact_list, key=lambda x: x[1], reverse=True)
        contacts_only = [item[0] for item in sorted_contacts]
        current_position = self.positions.get(person_id, (-1, -1))

        response = {
            "person_id": person_id,
            "contacts": contacts_only,
            "position": current_position
        }

        reply_to = properties.reply_to
        correlation_id = properties.correlation_id
        if reply_to:
            ch.basic_publish(
                exchange='',
                routing_key=reply_to,
                properties=pika.BasicProperties(correlation_id=correlation_id),
                body=json.dumps(response).encode('utf-8')
            )
            print(f"[Tracker] Response for '{person_id}' sent with correlation_id {correlation_id}")
        else:
            print(f"[Tracker] No reply_to specified for query {person_id}")

    def on_config_request(self, ch, method, properties, body):
        """
        Respond to configuration requests by sending the board size.
        """
        response = {"board_size": self.board_size}
        self.channel.basic_publish(
            exchange='',
            routing_key=CONFIG_RESPONSE_QUEUE,
            body=json.dumps(response).encode('utf-8')
        )
        print("[Tracker] Board size request received. Sent =", self.board_size)

def main():
    print("=== Starting Tracker ===")
    host = input("Enter RabbitMQ host address [default 'localhost']: ") or "localhost"
    board_size_str = input("Enter board size (N x N) [default 10]: ") or "10"
    board_size = int(board_size_str)

    tracker = Tracker(host, board_size)
    tracker.start()

if __name__ == "__main__":
    main()
