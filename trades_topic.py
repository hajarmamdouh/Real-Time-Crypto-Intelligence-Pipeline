import json
import websocket
from confluent_kafka import Producer

# ---------------------
# Kafka Configuration
# ---------------------
conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'XGXDR43CWRIHV5HG',
    'sasl.password': 'cflthFPYEOhMt81diPo6zq/9+MUBOyLt6oFOvvKlw0B3De42LE23KIir+Elwegdw',
}

producer = Producer(conf)
topic_name = "trades_topic"

# ---------------------
# Binance WebSocket Feed
# ---------------------
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "adausdt"]
stream = "/".join([f"{s}@trade" for s in SYMBOLS])
socket = f"wss://stream.binance.com:9443/stream?streams={stream}"

def delivery_report(err, msg):
    """ Called once message delivered to Kafka """
    if err is not None:
        print("❌ Error delivering message:", err)
    else:
        print(f"✔ Sent to Kafka | Topic: {msg.topic()} | Offset: {msg.offset()}")

def on_message(ws, message):
    msg = json.loads(message)
    data = msg["data"]

    trade = {
        "symbol": data["s"],
        "price": float(data["p"]),
        "qty": float(data["q"]),
        "timestamp": data["T"]
    }

    print(f"{trade['symbol']} | Price: {trade['price']} | Qty: {trade['qty']}")

    # send to Kafka
    producer.produce(
        topic_name,
        key=trade["symbol"],
        value=json.dumps(trade),
        callback=delivery_report
    )
    producer.poll(0)

def on_open(ws):
    print("Connected to Binance and Kafka… Streaming live trades 🚀")

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

ws = websocket.WebSocketApp(
    socket,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)

ws.run_forever()