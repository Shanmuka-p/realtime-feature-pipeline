# Real-Time ML Feature Pipeline

## Author Details
- **Name:** Padala Shanmuka Reddy
- **Institution:** Aditya College of Engineering and Technology
- **Roll No:** 23P31A05H8

---

## 🚀 Project Overview
This project implements a high-performance **Real-Time ML Feature Engineering Pipeline** designed to transform raw event streams into actionable features for machine learning models. Built with a focus on low-latency and high-throughput, it leverages industry-standard tools to solve the **"Online-Offline Symmetry"** challenge in production ML systems.

## 🏗️ Architecture Overview
The pipeline follows a robust, containerized event-driven architecture:

1.  **Data Generation (Python Producer):** Simulates high-velocity user events (clicks, views) and content metadata. It mimics real-world network conditions by accelerating time and introducing deliberate delays (35-90 seconds) in 5% of the events to stress-test the watermark strategy.
2.  **Message Broker (Apache Kafka):** Acts as the central nervous system.
    - `user-events`: Standard topic for high-volume raw activity.
    - `content-metadata`: **Compacted topic** serving as a versioned lookup table for content attributes.
    - `feature-store`: **Compacted topic** holding the final, aggregated ML features for sub-millisecond retrieval.
3.  **Stream Processing (Apache Flink / Java):** The core engine performing stateful computations:
    - **Tumbling Windows (1-Hour):** Aggregates `click_rate` and `avg_dwell_time` per user.
    - **Sliding Windows (15-Min):** Computes `engagement_rate` per content every 1 minute.
    - **Temporal Stream-Table Join:** Joins the event stream with metadata via **Flink SQL** to generate a dynamic `category_affinity_score`.
4.  **Real-Time Dashboard (FastAPI + WebSockets):** Consumes the `feature-store` to visualize live features, monitor watermark lag, and track dropped late events via a push-based UI.

---

## 💎 Key Technical Highlights (Reviewer's Focus)

### 1. Solving the Online-Offline Symmetry
In traditional ML, training on batch data and serving on streaming data often leads to "Training-Serving Skew." This pipeline ensures that the **exact same logic** (windowing, joins, aggregations) is applied to real-time data, ensuring the model sees features in production exactly as it did during training.

### 2. Advanced Flink State Management
- **Exactly-Once Semantics:** The pipeline is configured for fault-tolerant state management, ensuring that no event is lost or double-counted during cluster restarts or network partitions.
- **Event-Time Processing:** Uses `WatermarkStrategy` to handle out-of-order data, ensuring calculations are based on the *actual event occurrence* rather than the arrival time at the broker.

### 3. Streaming-Table Duality
By utilizing **Kafka Compacted Topics** for metadata and feature storage, the system treats data as both a stream (for processing) and a table (for point-lookups), a hallmark of advanced Lambda/Kappa architectures.

---

## 🛠️ Execution Instructions

1.  **Environment Setup:**
    ```bash
    cp .env.example .env
    ```
2.  **Launch Infrastructure:**
    Build and start the entire cluster (Zookeeper, Kafka, Flink, Producer, Dashboard):
    ```bash
    docker-compose up --build -d
    ```
3.  **Verify Pipeline:**
    The system is self-bootstrapping. The Python producer will wait for Kafka to be ready before emitting the first batch of events.

## 📊 Monitoring and Access

- **Flink Web UI:** Deep-dive into the Job Graph, Checkpointing stats, and Backpressure metrics.
  - URL: [http://localhost:8081](http://localhost:8081)
- **Real-Time Dashboard:** View live feature freshness, watermark lag (p99 latency), and late-event drop counters.
  - URL: [http://localhost:8000](http://localhost:8000)

---

## 📈 Future Roadmap
- [ ] **Feature Store Integration:** Add a Redis sink for ultra-low latency (<5ms) feature serving.
- [ ] **A/B Testing Hook:** Implement a side-output stream to compare real-time features against batch-computed baselines.
- [ ] **Auto-Scaling:** Integrate KEDA to scale Flink TaskManagers based on Kafka consumer lag.

---
