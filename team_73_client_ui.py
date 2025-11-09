from flask import Flask, request, render_template_string, jsonify, send_file
import team_73_producer  # Your existing Kafka + image functions
import os
import time
import threading
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import json

app = Flask(__name__)

# Global state for monitoring
processing_jobs = {}
worker_status = {}
active_workers = set()

# Configuration
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

BROKER = "10.147.18.63:9092"

# HTML Template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Distributed Image Processing Pipeline</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        h1 {
            color: white;
            text-align: center;
            margin-bottom: 30px;
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .dashboard {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .card {
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
        }
        .card h2 {
            color: #667eea;
            margin-bottom: 15px;
            font-size: 1.3em;
        }
        .upload-section {
            background: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }
        .file-input-wrapper {
            position: relative;
            overflow: hidden;
            display: inline-block;
            width: 100%;
        }
        .file-input-wrapper input[type=file] {
            position: absolute;
            left: -9999px;
        }
        .file-input-label {
            display: block;
            padding: 15px;
            background: #f8f9fa;
            border: 2px dashed #667eea;
            border-radius: 8px;
            text-align: center;
            cursor: pointer;
            transition: all 0.3s;
        }
        .file-input-label:hover {
            background: #e9ecef;
            border-color: #764ba2;
        }
        .btn {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 12px 30px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            margin-top: 15px;
            width: 100%;
            transition: transform 0.2s;
        }
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
        }
        .btn:disabled {
            background: #ccc;
            cursor: not-allowed;
            transform: none;
        }
        .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        .status-active { background: #28a745; }
        .status-inactive { background: #dc3545; animation: none; }
        .status-processing { background: #ffc107; }
        .worker-list, .job-list {
            max-height: 300px;
            overflow-y: auto;
        }
        .worker-item, .job-item {
            padding: 10px;
            border-bottom: 1px solid #e9ecef;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .worker-item:last-child, .job-item:last-child {
            border-bottom: none;
        }
        .progress-bar {
            width: 100%;
            height: 25px;
            background: #e9ecef;
            border-radius: 12px;
            overflow: hidden;
            margin: 10px 0;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            transition: width 0.3s;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
            font-size: 12px;
        }
        .metric {
            text-align: center;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 8px;
            margin: 10px 0;
        }
        .metric-value {
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }
        .metric-label {
            color: #6c757d;
            margin-top: 5px;
        }
        .image-preview {
            max-width: 100%;
            max-height: 300px;
            margin: 15px 0;
            border-radius: 8px;
            display: none;
        }
        .alert {
            padding: 15px;
            border-radius: 8px;
            margin: 15px 0;
        }
        .alert-success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .alert-error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        .timestamp {
            font-size: 0.85em;
            color: #6c757d;
        }
        #selectedFileName {
            margin-top: 10px;
            color: #667eea;
            font-weight: 500;
        }
        .tile-counter {
            font-size: 0.9em;
            color: #6c757d;
            margin-top: 5px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>\U0001f5bc\ufe0f Distributed Image Processing Pipeline</h1>
        
        <!-- Dashboard Stats -->
        <div class="dashboard">
            <div class="card">
                <h2>System Status</h2>
                <div class="metric">
                    <div class="metric-value" id="activeWorkers">0</div>
                    <div class="metric-label">Active Workers</div>
                </div>
                <div class="metric">
                    <div class="metric-value" id="processingJobs">0</div>
                    <div class="metric-label">Processing Jobs</div>
                </div>
            </div>
            
            <div class="card">
                <h2>Worker Status</h2>
                <div class="worker-list" id="workerList">
                    <p style="text-align: center; color: #6c757d;">Listening for workers...</p>
                </div>
            </div>
            
            <div class="card">
                <h2>Recent Jobs</h2>
                <div class="job-list" id="jobList">
                    <p style="text-align: center; color: #6c757d;">No jobs yet</p>
                </div>
            </div>
        </div>
        
        <!-- Upload Section -->
        <div class="upload-section">
            <h2 style="color: #667eea; margin-bottom: 20px;">Upload Image for Processing</h2>
            <form id="uploadForm" enctype="multipart/form-data">
                <div class="file-input-wrapper">
                    <input type="file" id="fileInput" name="file" accept="image/*" required>
                    <label for="fileInput" class="file-input-label">
                        \U0001f4c1 Click to select an image or drag and drop
                    </label>
                </div>
                <div id="selectedFileName"></div>
                <img id="imagePreview" class="image-preview" alt="Preview">
                <button type="submit" class="btn" id="submitBtn">Process Image</button>
            </form>
            <div id="uploadStatus"></div>
            
            <!-- Processing Progress -->
            <div id="processingSection" style="display: none; margin-top: 20px;">
                <h3 style="color: #667eea;">Processing Progress</h3>
                <div class="progress-bar">
                    <div class="progress-fill" id="progressBar">0%</div>
                </div>
                <p id="progressText" style="text-align: center; margin-top: 10px;"></p>
            </div>
            
            <!-- Result Section -->
            <div id="resultSection" style="display: none; margin-top: 20px;">
                <div class="alert alert-success">
                    \u2705 Processing Complete!
                </div>
                <img id="resultImage" style="max-width: 100%; border-radius: 8px; margin: 15px 0;">
                <a href="#" id="downloadLink" class="btn">Download Processed Image</a>
            </div>
        </div>
    </div>

    <script>
        let pollInterval;
        
        // File input handling
        document.getElementById('fileInput').addEventListener('change', function(e) {
            const file = e.target.files[0];
            if (file) {
                document.getElementById('selectedFileName').textContent = '\U0001f4c4 ' + file.name;
                
                // Preview image
                const reader = new FileReader();
                reader.onload = function(e) {
                    const preview = document.getElementById('imagePreview');
                    preview.src = e.target.result;
                    preview.style.display = 'block';
                }
                reader.readAsDataURL(file);
            }
        });
        
        // Form submission
        document.getElementById('uploadForm').addEventListener('submit', async function(e) {
            e.preventDefault();
            
            const formData = new FormData(this);
            const submitBtn = document.getElementById('submitBtn');
            const uploadStatus = document.getElementById('uploadStatus');
            const processingSection = document.getElementById('processingSection');
            const resultSection = document.getElementById('resultSection');
            
            submitBtn.disabled = true;
            submitBtn.textContent = 'Uploading...';
            uploadStatus.innerHTML = '';
            processingSection.style.display = 'none';
            resultSection.style.display = 'none';
            
            try {
                const response = await fetch('/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const data = await response.json();
                
                if (data.success) {
                    processingSection.style.display = 'block';
                    submitBtn.textContent = 'Processing...';
                    startPolling(data.job_id);
                } else {
                    uploadStatus.innerHTML = `<div class="alert alert-error">${data.error}</div>`;
                    submitBtn.disabled = false;
                    submitBtn.textContent = 'Process Image';
                }
            } catch (error) {
                uploadStatus.innerHTML = `<div class="alert alert-error">Error: ${error.message}</div>`;
                submitBtn.disabled = false;
                submitBtn.textContent = 'Process Image';
            }
        });
        
        // Poll job status
        function startPolling(jobId) {
            pollInterval = setInterval(async () => {
                try {
                    const response = await fetch(`/job_status/${jobId}`);
                    const data = await response.json();
                    
                    const progressBar = document.getElementById('progressBar');
                    const progressText = document.getElementById('progressText');
                    const submitBtn = document.getElementById('submitBtn');
                    const resultSection = document.getElementById('resultSection');
                    
                    progressBar.style.width = data.progress + '%';
                    progressBar.textContent = data.progress + '%';
                    progressText.textContent = data.status + ' (' + data.completed + '/' + data.total + ' tiles)';
                    
                    if (data.complete) {
                        clearInterval(pollInterval);
                        document.getElementById('resultImage').src = `/download/${jobId}`;
                        resultSection.style.display = 'block';
                        document.getElementById('downloadLink').href = `/download/${jobId}`;
                        submitBtn.disabled = false;
                        submitBtn.textContent = 'Process Image';
                    } else if (data.error) {
                        clearInterval(pollInterval);
                        document.getElementById('uploadStatus').innerHTML = 
                            `<div class="alert alert-error">Error: ${data.error}</div>`;
                        submitBtn.disabled = false;
                        submitBtn.textContent = 'Process Image';
                    }
                } catch (error) {
                    console.error('Polling error:', error);
                }
            }, 500);
        }
        
        // Update dashboard
        async function updateDashboard() {
            try {
                const response = await fetch('/dashboard_data');
                const data = await response.json();
                
                document.getElementById('activeWorkers').textContent = data.active_workers;
                document.getElementById('processingJobs').textContent = data.processing_jobs;
                
                // Update worker list
                const workerList = document.getElementById('workerList');
                if (data.workers.length > 0) {
                    workerList.innerHTML = data.workers.map(w => `
                        <div class="worker-item">
                            <span>
                                <span class="status-indicator status-${w.status}"></span>
                                ${w.name}
                            </span>
                            <span class="timestamp">${w.last_seen}</span>
                        </div>
                    `).join('');
                } else {
                    workerList.innerHTML = '<p style="text-align: center; color: #6c757d;">Listening for workers...</p>';
                }
                
                // Update job list
                const jobList = document.getElementById('jobList');
                if (data.jobs.length > 0) {
                    jobList.innerHTML = data.jobs.map(j => `
                        <div class="job-item">
                            <div>
                                <div>Job ${j.id}</div>
                                <div class="tile-counter">${j.completed}/${j.total} tiles</div>
                            </div>
                            <span>
                                <span class="status-indicator status-${j.status}"></span>
                                ${j.progress}%
                            </span>
                        </div>
                    `).join('');
                } else {
                    jobList.innerHTML = '<p style="text-align: center; color: #6c757d;">No jobs yet</p>';
                }
            } catch (error) {
                console.error('Dashboard update error:', error);
            }
        }
        
        // Update dashboard every 2 seconds
        setInterval(updateDashboard, 2000);
        updateDashboard();
    </script>
</body>
</html>
'''

def monitor_heartbeats():
    """Background thread to monitor worker heartbeats"""
    try:
        consumer = Consumer({
            'bootstrap.servers': BROKER,
            'group.id': 'ui-heartbeat-monitor',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe(['heartbeats'])
        
        print("\U0001f442 Listening for worker heartbeats...")
        
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"\u274c Consumer error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                worker_id = data.get('worker_id', 'unknown')
                
                worker_status[worker_id] = {
                    'last_seen': datetime.now(),
                    'status': 'active'
                }
                active_workers.add(worker_id)
                
            except Exception as e:
                print(f"\u26a0\ufe0f Error processing heartbeat: {e}")
            
            # Clean up stale workers (no heartbeat in 10 seconds)
            now = datetime.now()
            stale_workers = [
                wid for wid, info in worker_status.items()
                if (now - info['last_seen']).total_seconds() > 10
            ]
            for wid in stale_workers:
                active_workers.discard(wid)
                if wid in worker_status:
                    worker_status[wid]['status'] = 'inactive'
                    
    except Exception as e:
        print(f"\u274c Heartbeat monitor error: {e}")

@app.route('/')
def home():
    return render_template_string(HTML_TEMPLATE)

@app.route('/upload', methods=['POST'])
def upload():
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file provided'})
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'})
        
        # Generate unique job ID
        job_id = f"job_{int(time.time() * 1000)}"
        filename = os.path.join(UPLOAD_FOLDER, f"{job_id}.jpg")
        file.save(filename)
        
        # Split image into tiles
        tiles, size = team_73_producer.split_image(filename)
        
        # Initialize job tracking BEFORE publishing
        processing_jobs[job_id] = {
            'total_tiles': len(tiles),
            'completed_tiles': 0,
            'size': size,
            'status': 'processing',
            'start_time': datetime.now(),
            'filename': filename
        }
        
        # Publish tiles to Kafka with job_id
        team_73_producer.publish_tiles(tiles, job_id)
        
        # Start background thread to collect results
        threading.Thread(target=collect_results_async, args=(job_id,), daemon=True).start()
        
        return jsonify({'success': True, 'job_id': job_id})
    
    except Exception as e:
        print(f"\u274c Upload error: {e}")
        return jsonify({'success': False, 'error': str(e)})

def collect_results_async(job_id):
    """Collect processed tiles asynchronously"""
    try:
        job = processing_jobs[job_id]
        total_tiles = job['total_tiles']
        
        # Create consumer for this job
        consumer = Consumer({
            'bootstrap.servers': BROKER,
            'group.id': f'ui-results-{job_id}',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe(['results'])
        
        processed = []
        timeout_start = time.time()
        timeout = 300  # 5 minutes timeout
        
        print(f"\u23f3 Job {job_id}: Waiting for {total_tiles} processed tiles...")
        
        while len(processed) < total_tiles:
            # Check timeout
            if time.time() - timeout_start > timeout:
                processing_jobs[job_id]['status'] = 'error'
                processing_jobs[job_id]['error'] = 'Timeout waiting for results'
                print(f"\u23f0 Job {job_id}: Timeout")
                break
            
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"\u274c Consumer error: {msg.error()}")
                continue
            
            try:
                tile = json.loads(msg.value().decode('utf-8'))
                
                # Check if this tile belongs to our job
                tile_job_id = tile.get('job_id')
                if tile_job_id and tile_job_id != job_id:
                    continue  # Skip tiles from other jobs
                
                processed.append(tile)
                processing_jobs[job_id]['completed_tiles'] = len(processed)
                print(f"\U0001f4e5 Job {job_id}: Received {len(processed)}/{total_tiles} tiles")
                
            except Exception as e:
                print(f"\u26a0\ufe0f Error processing result: {e}")
        
        consumer.close()
        
        # Reconstruct image if all tiles received
        if len(processed) == total_tiles:
            size = job['size']
            output_path = os.path.join(OUTPUT_FOLDER, f"{job_id}.jpg")
            
            team_73_producer.reconstruct_image(processed, size, output_path)
            
            processing_jobs[job_id]['status'] = 'complete'
            processing_jobs[job_id]['output_path'] = output_path
            print(f"\u2705 Job {job_id}: Complete!")
        
    except Exception as e:
        print(f"\u274c Job {job_id} error: {e}")
        processing_jobs[job_id]['status'] = 'error'
        processing_jobs[job_id]['error'] = str(e)

@app.route('/job_status/<job_id>')
def job_status(job_id):
    if job_id not in processing_jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    job = processing_jobs[job_id]
    progress = int((job['completed_tiles'] / job['total_tiles']) * 100) if job['total_tiles'] > 0 else 0
    
    status_text = {
        'processing': 'Processing tiles',
        'complete': 'Complete',
        'error': 'Error'
    }.get(job['status'], 'Unknown')
    
    response = {
        'progress': progress,
        'status': status_text,
        'complete': job['status'] == 'complete',
        'total': job['total_tiles'],
        'completed': job['completed_tiles']
    }
    
    if job['status'] == 'error' and 'error' in job:
        response['error'] = job['error']
    
    return jsonify(response)

@app.route('/download/<job_id>')
def download(job_id):
    if job_id not in processing_jobs or 'output_path' not in processing_jobs[job_id]:
        return "File not found", 404
    
    return send_file(
        processing_jobs[job_id]['output_path'],
        mimetype='image/jpeg',
        as_attachment=True,
        download_name=f"processed_{job_id}.jpg"
    )

@app.route('/dashboard_data')
def dashboard_data():
    # Clean up stale workers
    now = datetime.now()
    active = [
        wid for wid, info in worker_status.items()
        if (now - info['last_seen']).total_seconds() <= 10
    ]
    
    workers = [
        {
            'name': wid,
            'status': 'active' if wid in active else 'inactive',
            'last_seen': info['last_seen'].strftime('%H:%M:%S')
        }
        for wid, info in sorted(worker_status.items(), key=lambda x: x[1]['last_seen'], reverse=True)
    ]
    
    jobs = [
        {
            'id': jid,
            'progress': int((job['completed_tiles'] / job['total_tiles']) * 100) if job['total_tiles'] > 0 else 0,
            'status': 'processing' if job['status'] == 'processing' else 'active',
            'completed': job['completed_tiles'],
            'total': job['total_tiles']
        }
        for jid, job in sorted(processing_jobs.items(), key=lambda x: x[1]['start_time'], reverse=True)[:5]
    ]
    
    return jsonify({
        'active_workers': len(active),
        'processing_jobs': len([j for j in processing_jobs.values() if j['status'] == 'processing']),
        'workers': workers,
        'jobs': jobs
    })

if __name__ == '__main__':
    # Start heartbeat monitoring in background
    monitor_thread = threading.Thread(target=monitor_heartbeats, daemon=True)
    monitor_thread.start()
    
    print("\U0001f680 Starting Image Processing Pipeline UI...")
    print(f"\U0001f517 Broker: {BROKER}")
    print("\U0001f4ca Dashboard available at http://0.0.0.0:5000")
    print("=" * 50)
    
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)