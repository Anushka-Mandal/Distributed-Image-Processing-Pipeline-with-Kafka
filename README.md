# 🖼️ Distributed Image Processing Pipeline with Kafka

## Project Objective
To design and implement a **distributed image processing pipeline** using **Apache Kafka** for asynchronous communication between master and worker nodes.  
The system enables **parallel image processing**, **fault tolerance**, and **scalable task distribution** across multiple workers.

---

## System Overview
The pipeline consists of **four main components** connected via Kafka topics:

1. **Node 1 – Client & Master**
   - Flask based UI for uploading and retrieving processed images.
   - Splits input images into tiles.
   - Publishes each tile as a task to Kafka.
   - Collects processed tiles from workers.
   - Reconstructs the final image.
   - Monitors worker health via heartbeat messages.

2. **Node 2 – Kafka Broker**
   - Acts as the message broker between master and workers.
   - Manages three Kafka topics:
     - `tasks` → for image tile tasks
     - `results` → for processed image tiles
     - `heartbeats` → for worker health monitoring

3. **Node 3 & Node 4 – Worker 1 and Worker 2**
   - Consume tile tasks from Kafka.
   - Apply image processing operations (grayscale).
   - Publish processed tiles to the `results` topic.
   - Send periodic heartbeat signals to `heartbeats`.

---

## Kafka Topics

| Topic | Purpose |
|--------|----------|
| `tasks` | To distribute image tile processing tasks to workers |
| `results` | To collect processed tiles from workers |
| `heartbeats` | To track worker activity and health |

---

## Project Structure

```

project3_distributed_image_pipeline/
│
├── team_73_client_ui.py       # Flask client interface + master logic
├── team_73_producer.py        # Handles image splitting, task publishing
├── team_73_worker1.py         # Worker 1 node (tile processing)
├── 73_worker2.py              # Worker 2 node (tile processing)
├── team_73_broker.txt         # Kafka topic setup and broker info
├── README.md                  # Project documentation

````

---

## Setup Instructions

### 1️⃣ Prerequisites
Make sure you have the following installed:
- **Python 3.8+**
- **Apache Kafka & Zookeeper**
- **pip** (Python package manager)

### 2️⃣ Install Dependencies
Clone the repository and install all dependencies:
```bash
git clone https://github.com/Anushka-Mandal/73_project3_BD.git
cd 73_project3_BD
pip install kafka-python confluent-kafka flask opencv-python pillow numpy
````

### 3️⃣ Start Kafka and Zookeeper

In separate terminals:

```bash
# Start Zookeeper
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker
cd /opt/kafka
bin/kafka-server-start.sh config/server.properties
```

### 4️⃣ Create Kafka Topics

Run the following commands:
Our broker runs on a remote machine with Broker IP:10.147.18.63 on port 9092

```bash
kafka-topics.sh --create --topic tasks --bootstrap-server 10.147.18.63:9092 --partitions 2 --replication-factor 1
kafka-topics.sh --create --topic results --bootstrap-server 10.147.18.63:9092 --partitions 2 --replication-factor 1
kafka-topics.sh --create --topic heartbeats --bootstrap-server 10.147.18.63:9092 --partitions 1 --replication-factor 1
```

### 5️⃣ Run the Nodes

#### Step 1: Start the Broker

Ensure Kafka and Zookeeper are running.

#### Step 2: Start Workers

In two separate terminals:

```bash
python team_73_worker1.py
python 73_worker2.py
```

#### Step 3: Start Master Node

```bash
python team_73_client_ui.py
```

#### Step 4: Access the Web Interface

Open your browser and visit:
👉 **[http://localhost:5000](http://localhost:5000)**

Here you can:

* Upload an image (min. 1024×1024)
* View processing progress
* Download the processed result

---

## 📊 Example Processing Flow

1. User uploads an image via Flask UI.
2. Master splits the image into 512×512 tiles.
3. Tiles are sent as messages to `tasks` topic.
4. Worker nodes consume tiles, process them (e.g., grayscale conversion).
5. Processed tiles are sent to `results` topic.
6. Master reconstructs the complete processed image.
7. The final image is displayed on the UI.
