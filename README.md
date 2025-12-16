# Distributed MES Lab (Backend)

This repository is a **personal study & portfolio project** that demonstrates the design and implementation of a **distributed Manufacturing Execution System (MES) backend**.

The project focuses on **workcell-level monitoring, device-level analytics, and time-series analysis pipelines**, inspired by real-world manufacturing environments, while intentionally **sanitizing all proprietary logic, schemas, and data**.

---

## 🔍 Project Overview

- **Domain**: Manufacturing / MES / Digital Twin
- **Type**: Backend API & analytics service
- **Purpose**:  
  - Practice scalable backend design for manufacturing data  
  - Demonstrate real-time & historical analytics patterns  
  - Provide a clean, reviewable codebase for portfolio evaluation

This repository is **not intended to run as-is in production**, but to showcase:
- API structure
- Query patterns
- Data modeling decisions
- Practical backend engineering skills

---

## 🧠 Key Features

### 1. Workcell-Level Monitoring
- Aggregated KPIs (OEE, throughput, cycle time, takt adherence)
- Time-window–based calculations
- Freshness validation for real-time dashboards

### 2. Device-Level Analytics
- Device type / device name filtering
- Per-device and per-type aggregation
- Defect rate & cycle time analysis

### 3. Time-Series Analysis
- SQL-based aggregation logic
- Windowed queries and anchor-time alignment
- Fallback simulation logic when data is unavailable

### 4. Clean API Design
- FastAPI router separation by domain
- Explicit query validation
- Clear response payload structures

---

## 🛠 Tech Stack

- **Python**
- **FastAPI**
- **SQLAlchemy (Core + text queries)**
- **MySQL (assumed data source)**
- **InfluxDB (time-series design reference)**
- **Git / GitHub**

---

## 📁 Project Structure

