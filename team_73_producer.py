from confluent_kafka import Producer, Consumer
import cv2, numpy as np, json, base64

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

    print(f"🖼️ Image split into {len(tiles)} tiles ({w}×{h})")
    return tiles, (w, h)

# -----------------------------
# Kafka delivery callback
# -----------------------------
def acked(err, msg):
    if err is not None:
        print(f"❌ Failed to deliver message: {err}")
    else:
        print(f"✅ Tile delivered to {msg.topic()} [partition {msg.partition()}]")

# -----------------------------
# Publish tiles (Manual partition routing)
# -----------------------------
def publish_tiles(tiles, job_id=None):
    """Send tiles to correct Kafka partitions based on x-coordinate"""
    for i, tile in enumerate(tiles, 1):
        if job_id:
            tile['job_id'] = job_id

        # MANUAL PARTITIONING: left half → partition 0, right half → partition 1
        partition = 0 if tile['x'] < 512 else 1

        producer.produce(
            topic='tasks',
            value=json.dumps(tile).encode('utf-8'),
            partition=partition,       # ✅ explicit partition routing
            callback=acked
        )
        print(f"📤 Sent tile {i}/{len(tiles)} → partition {partition}")

    producer.flush()
    print(f"🎯 All {len(tiles)} tiles published to Kafka")

# -----------------------------
# Collect processed results
# -----------------------------
def collect_results(total_tiles, job_id=None):
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': f'master-group-{job_id}' if job_id else 'master-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['results'])

    processed = []
    print(f"⏳ Waiting for {total_tiles} processed tiles...")

    while len(processed) < total_tiles:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        try:
            tile = json.loads(msg.value().decode('utf-8'))
            if job_id and tile.get('job_id') != job_id:
                continue
            processed.append(tile)
            print(f"📥 Received {len(processed)}/{total_tiles} tiles")
        except Exception as e:
            print(f"⚠️ Error processing tile: {e}")

    consumer.close()
    return processed

# -----------------------------
# Reconstruct final image
# -----------------------------
def reconstruct_image(processed_tiles, size, output_path="output.jpg"):
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
    print(f"🎯 Final image reconstructed as {output_path}")

# -----------------------------
# Stand-alone usage
# -----------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python producer.py <image_path>")
        sys.exit(1)

    img_path = sys.argv[1]
    tiles, size = split_image(img_path)
    publish_tiles(tiles, job_id="manual_split_test")
    results = collect_results(len(tiles), job_id="manual_split_test")
    reconstruct_image(results, size)
    print("✅ Processing complete!")

