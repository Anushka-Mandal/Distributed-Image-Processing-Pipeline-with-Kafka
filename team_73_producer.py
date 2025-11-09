from confluent_kafka import Producer, Consumer
import cv2, numpy as np, json, base64, time

BROKER = "10.147.18.63:9092"  # Broker machine
producer = Producer({'bootstrap.servers': BROKER})

# -----------------------------
# Image splitting
# -----------------------------
def split_image(img_path, tile_size=512):
    """Split image into tiles for distributed processing"""
    img = cv2.imread(img_path)
    if img is None:
        raise ValueError(f"Failed to read image: {img_path}")

    h, w, _ = img.shape
    tiles = []

    for y in range(0, h, tile_size):
        for x in range(0, w, tile_size):
            tile = img[y:y+tile_size, x:x+tile_size]
            _, buffer = cv2.imencode('.jpg', tile)
            tile_data = base64.b64encode(buffer).decode('utf-8')
            tiles.append({'x': x, 'y': y, 'data': tile_data})

    print(f"\U0001f5bc\ufe0f Image split into {len(tiles)} tiles ({w}\u00d7{h})")
    return tiles, (w, h)

# -----------------------------
# Kafka delivery callback
# -----------------------------
def acked(err, msg):
    if err is not None:
        print(f"\u274c Failed to deliver message: {err}")
    else:
        print(f"\u2705 Tile delivered to {msg.topic()} [partition {msg.partition()}]")

# -----------------------------
# Publish tiles (Manual partition routing)
# -----------------------------
def publish_tiles(tiles, job_id):
    """Send tiles to correct Kafka partitions based on x-coordinate"""
    print(f"\U0001f680 Starting Job ID: {job_id}")
    for i, tile in enumerate(tiles, 1):
        tile['job_id'] = job_id

        # Manual partitioning: left \u2192 0, right \u2192 1
        partition = 0 if tile['x'] < 512 else 1

        producer.produce(
            topic='tasks',
            value=json.dumps(tile).encode('utf-8'),
            partition=partition,
            callback=acked
        )
        print(f"\U0001f4e4 Sent tile {i}/{len(tiles)} \u2192 partition {partition}")

    producer.flush()
    print(f"\U0001f3af All {len(tiles)} tiles published to Kafka for job {job_id}")

# -----------------------------
# Collect processed results
# -----------------------------
def collect_results(total_tiles, job_id):
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': f'master-group-{job_id}',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['results'])

    processed = []
    worker_count = {}
    print(f"\u23f3 Waiting for {total_tiles} processed tiles for {job_id}...")

    while len(processed) < total_tiles:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"\u274c Consumer error: {msg.error()}")
            continue

        try:
            tile = json.loads(msg.value().decode('utf-8'))
            if tile.get('job_id') != job_id:
                continue

            x, y = tile['x'], tile['y']
            worker = tile.get('worker_id', 'unknown')
            print(f"\U0001f4e6 Tile ({x},{y}) processed by {worker}")

            worker_count[worker] = worker_count.get(worker, 0) + 1
            processed.append(tile)
            print(f"\U0001f4e5 Job {job_id}: Received {len(processed)}/{total_tiles} tiles")

        except Exception as e:
            print(f"\u26a0\ufe0f Error processing tile: {e}")

    print("\U0001f9fe Worker contribution summary:")
    for worker, count in worker_count.items():
        print(f"   \u2022 {worker}: {count} tile(s) processed")

    consumer.close()
    return processed

# -----------------------------
# Reconstruct final image
# -----------------------------
def reconstruct_image(processed_tiles, size, output_path):
    w, h = size
    final_img = np.zeros((h, w, 3), dtype=np.uint8)

    for tile in processed_tiles:
        x, y = tile['x'], tile['y']
        data = base64.b64decode(tile['data'])
        img_array = np.frombuffer(data, np.uint8)
        tile_img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        th, tw = tile_img.shape[:2]
        final_img[y:y+th, x:x+tw] = tile_img

    cv2.imwrite(output_path, final_img)
    print(f"\U0001f3af Final image reconstructed as {output_path}")
    print(f"\u2705 Job {output_path.split('/')[-1].replace('.jpg','')} complete!")

# -----------------------------
# Stand-alone usage
# -----------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python producer.py <image_path> [job_id]")
        sys.exit(1)

    img_path = sys.argv[1]
    job_id = sys.argv[2] if len(sys.argv) > 2 else f"job_{int(time.time() * 1000)}"
    tiles, size = split_image(img_path)
    publish_tiles(tiles, job_id=job_id)
    results = collect_results(len(tiles), job_id=job_id)
    reconstruct_image(results, size, f"outputs/{job_id}.jpg")
