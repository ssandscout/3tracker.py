# Tracker Module: Receives position messages, tracks contacts, handles queries.
import pika
import json
from collections import defaultdict

# Queue/Exchange settings
QUERY_TOPIC = 'query'
CONFIG_REQUEST_QUEUE = 'config_request'
CONFIG_RESPONSE_QUEUE = 'config_response'
EXCHANGE_NAME = 'positions'  # Fanout exchange for position updates

class Tracker:
    def __init__(self, host, board_size):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

        # Declare exchange and bind a queue to receive position updates
        self.channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')
        result = self.channel.queue_declare(queue="", exclusive=False)
        self.position_queue = result.method.queue
        self.channel.queue_bind(exchange=EXCHANGE_NAME, queue=self.position_queue)

        # Declare queues for queries and config requests
        self.channel.queue_declare(queue=QUERY_TOPIC)
        self.channel.queue_declare(queue=CONFIG_REQUEST_QUEUE)
        self.channel.queue_declare(queue=CONFIG_RESPONSE_QUEUE)

        # Data structures for positions and contact history
        self.positions = {}  # { person_id: (x, y) }
        self.contacts = defaultdict(list)  # { person_id: [(other_id, timestamp), ...] }
        self.board_size = board_size

    def start(self):
        self.channel.basic_consume(queue=self.position_queue,
                                   on_message_callback=self.on_position_message,
                                   auto_ack=True)
        self.channel.basic_consume(queue=QUERY_TOPIC,
                                   on_message_callback=self.on_query_message,
                                   auto_ack=True)
        self.channel.basic_consume(queue=CONFIG_REQUEST_QUEUE,
                                   on_message_callback=self.on_config_request,
                                   auto_ack=True)
        print("[Tracker] Listening for messages...")
        self.channel.start_consuming()

    def on_position_message(self, ch, method, properties, body):
        """
        Message format: {"person_id": str, "x": int, "y": int, "timestamp": float}
        """
        try:
            msg = json.loads(body)
            pid = msg["person_id"]
            x, y, ts = msg["x"], msg["y"], msg["timestamp"]
        except (json.JSONDecodeError, KeyError):
            print("[Tracker] Invalid position message")
            return

        self.positions[pid] = (x, y)
        # Check if this position overlaps with any other
        for other_id, (ox, oy) in self.positions.items():
            if other_id != pid and ox == x and oy == y:
                self.contacts[pid].append((other_id, ts))
                self.contacts[other_id].append((pid, ts))

    def on_query_message(self, ch, method, properties, body):
        """
        Message format: {"person_id": str}
        Responds with: {"person_id": str, "contacts": [ids...], "position": (x, y)}
        """
        try:
            msg = json.loads(body)
            pid = msg["person_id"]
        except (json.JSONDecodeError, KeyError):
            print("[Tracker] Invalid query message")
            return

        contact_data = self.contacts.get(pid, [])
        # Sort by timestamp descending
        sorted_data = sorted(contact_data, key=lambda x: x[1], reverse=True)
        response = {
            "person_id": pid,
            "contacts": [c[0] for c in sorted_data],
            "position": self.positions.get(pid, (-1, -1))
        }

        if properties.reply_to:
            ch.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                body=json.dumps(response).encode('utf-8'),
                properties=pika.BasicProperties(correlation_id=properties.correlation_id)
            )
            print(f"[Tracker] Sent response for {pid}")
        else:
            print(f"[Tracker] No reply_to in query for {pid}")

    def on_config_request(self, ch, method, properties, body):
        """
        Responds with: {"board_size": <int>}
        """
        resp = {"board_size": self.board_size}
        self.channel.basic_publish(
            exchange='',
            routing_key=CONFIG_RESPONSE_QUEUE,
            body=json.dumps(resp).encode('utf-8')
        )
        print("[Tracker] Board size request handled")

def main():
    print("=== Starting Tracker ===")
    host = input("RabbitMQ host [default=localhost]: ") or "localhost"
    board_str = input("Board size [default=10]: ") or "10"
    board_size = int(board_str)

    tracker = Tracker(host, board_size)
    tracker.start()

if __name__ == "__main__":
    main()
