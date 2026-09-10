# System Architecture

## High-level flow
```text
User / Evaluator
  |
  v
Frontend Dashboard (React / Vite on Netlify)
  |
  v
Backend API (FastAPI Python Server)
  |
  +------------------> Database (SQLite / PostgreSQL & Audit Logs)
  |
  v
Automated Scraping Engine & ML Service (Anomalies & 7-Day Forecast)
  |
  v
Data Ingestion (IndiGo, Air India, SpiceJet, Akasa Air Data)
  |
  v
Aggregated National Airfare Price Index (CPI) Output
```

## Components

### Frontend
Handles user interaction, parameter filtering (by airline, city-pair, advance purchase windows), and renders real-time visual charts like the Airfare Price Trend and Advance Purchase Topology.

### Backend API
Built with FastAPI to receive requests, process route logic, authenticate queries, and coordinate application logic between the data layer and client.

### Machine Learning & Analytics Service
Processes ingested flight transactions to detect pricing anomalies, track moving averages, and generate forward-looking airfare forecasts.

### Database & Audit Logs
Stores historical transaction records, separating base fares from variable taxes and fees, and tracks verification logs for MoSPI reporting.
