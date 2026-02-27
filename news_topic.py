# news_to_kafka.py

import signal
import sys
import requests
import json
from datetime import datetime
from confluent_kafka import Producer

# ==========================================================
# 🔐 CONFIGURATION CONFLUENT CLOUD (REMPLACE PAR TES CLES)
# ==========================================================

API_KEY = "XGXDR43CWRIHV5HG"
API_SECRET = "cflthFPYEOhMt81diPo6zq/9+MUBOyLt6oFOvvKlw0B3De42LE23KIir+Elwegdw"
BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
TOPIC = "news_topic"

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
    'client.id': 'news-producer',
    'acks': 'all',
    'retries': 5
}

producer = Producer(conf)

# =============================
# NewsAPI configuration
# =============================
NEWSAPI_KEY = "b61e2b4006ce493599bb179be55cfc89"
NEWSAPI_URL = "https://newsapi.org/v2/everything"

NEWS_PARAMS = {
    "q": "(bitcoin OR ethereum) AND (price OR trading OR market OR volatility)",
    "sources": "reuters,bloomberg,cnbc,cointelegraph,financial-times",
    "language": "en",
    "sortBy": "publishedAt",
    "pageSize": 5,
    "apiKey": NEWSAPI_KEY
}

# =============================
# Delivery report callback
# =============================
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent: {msg.key().decode()} -> partition {msg.partition()}")

# =============================
# Graceful shutdown
# =============================
def shutdown(sig, frame):
    print("\n⏳ Flushing remaining messages...")
    producer.flush()
    print("✅ Shutdown complete")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)

# =============================
# Fetch & send news
# =============================
def fetch_and_send_news():
    try:
        response = requests.get(NEWSAPI_URL, params=NEWS_PARAMS)
        if response.status_code != 200:
            print("HTTP Error:", response.status_code)
            print(response.text)
            return

        data = response.json()
        if data["status"] != "ok":
            print("API Error:", data)
            return

        print(f"\nFetched {len(data['articles'])} news at {datetime.utcnow().isoformat()}\n")
        for article in data["articles"]:
            news_data = {
                "title": article["title"],
                "source": article["source"]["name"],
                "description": article["description"],
                "url": article["url"],
                "publishedAt": article["publishedAt"]
            }

            producer.produce(
                TOPIC,
                key=news_data["source"],
                value=json.dumps(news_data),
                callback=delivery_report
            )
            producer.poll(0)

    except Exception as e:
        print("❌ Error fetching/sending news:", e)

# =============================
# Main loop: fetch every 5 minutes
# =============================
if __name__ == "__main__":
    while True:
        fetch_and_send_news()