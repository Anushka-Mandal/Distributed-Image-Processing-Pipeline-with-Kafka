from confluent_kafka import Consumer, Producer, TopicPartition
import cv2, numpy as np, json, base64, time, threading, datetime

BROKER = "10.147.18.63:9092"
WORKER_ID = "worker_083"

producer = Producer({'bootstrap.servers': BROKER})
consumer = Consumer({
    'bootstrap.servers': BROKER,
    'group.id': 'grayscale_workers',
    'auto.offset.reset': 'earliest'
})

# Assign only partition 0 of topic 'tasks'
consumer.assign([TopicPartition('tasks', 0)])

processed_count = 0

def send_heartbeat():
    while True:
        try:
            heartbeat = {
                'worker_id': WORKER_ID,
                'timestamp': time.time(),
                'status': 'active',
                'partition': 0,
                'half': 'LEFT'
            }
            producer.produce('heartbeats', json.dumps(heartbeat).encode('utf-8'))
            producer.flush()
            print(f"\U0001f493 [{WORKER_ID}] Heartbeat sent")
        except Exception as e:
            print(f"\u26a0\ufe0f [{WORKER_ID}] Heartbeat failed: {e}")
        time.sleep(5)

def process_tile(tile_data):
    img_bytes = base64.b64decode(tile_data)
    img_array = np.frombuffer(img_bytes, np.uint8)
    img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    processed = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
    _, buffer = cv2.imencode('.jpg', processed)
    return base64.b64encode(buffer).decode('utf-8')

def main():
    global processed_count
    print("=" * 60)
    print(f"\U0001f680 {WORKER_ID} (LEFT-HALF) started \u2014 consuming partition 0")
    print("=" * 60)

    threading.Thread(target=send_heartbeat, daemon=True).start()

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"\u274c [{WORKER_ID}] Consumer error: {msg.error()}")
            continue

        task = json.loads(msg.value().decode('utf-8'))
        x, y = task['x'], task['y']
        job_id = task.get('job_id', 'unknown')

        ts = datetime.datetime.now().strftime('%H:%M:%S.%f')
        print(f"\U0001f4e5 [{WORKER_ID}] Got tile ({x},{y}) at {ts}")

        start = time.time()
        processed_data = process_tile(task['data'])
        process_time = time.time() - start

        result = {
            'x': x, 'y': y,
            'data': processed_data,
            'job_id': job_id,
            'worker_id': WORKER_ID,
            'processing_time': process_time
        }

        producer.produce('results', json.dumps(result).encode('utf-8'))
        producer.flush()
        processed_count += 1
        print(f"\u2705 [{WORKER_ID}] Tile ({x},{y}) processed in {process_time:.3f}s (Total: {processed_count})")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\U0001f6d1 [{WORKER_ID}] Stopped gracefully")
        consumer.close()