# SIH 2026 Project Repository: Airfare CPI Dashboard

## 1. Project Information
- **Project Title:** National Airfare Price Index (Airfare CPI) Dashboard
- **PS ID:** 26056
- **PS Title:** Real-time airfare tracking and Consumer Price Index (CPI) analysis dashboard
- **Category:** Software
- **Theme:** Smart Analytics / Data Governance
- - **Live Demo:** [https://airfareindexsih.netlify.app/](https://airfareindexsih.netlify.app/)

## 2. Problem Statement
The Ministry of Statistics and Programme Implementation (MoSPI) requires a robust methodology to track and analyze the highly volatile Indian aviation sector. Manual data collection fails to capture real-time anomalies or effectively separate base fares from variable taxes and fees, making it difficult to generate an accurate National Airfare Price Index. 

## 3. Proposed Solution
This project provides a Real-Time Price Index Analysis platform tailored for MoSPI. The system actively scrapes and ingests live pricing data across 5 primary trunk corridors and regional UDAN routes. It establishes a Current Airfare CPI (with a revised base year of 2012=100) and maps flight data against a 7-day moving average to automatically flag high-volatility pricing anomalies.

## 4. Key Features
- **Real-Time CPI Tracking:** Live dashboard displaying the Current Airfare CPI and daily percentage changes.
- **Dynamic Data Filtering:** Allows users to filter searches by specific airlines (IndiGo, Air India, SpiceJet, Akasa), city-pairs (e.g., DEL-BOM, CCU-PAT), and advance purchase windows (T+1, T+7, T+15, and T+30 Days).
- **Interactive Visualizations:** Includes an Airfare Price Trend & 7-Day Forecast chart, Airline Base Fare Averages, and an Advance Purchase Topology graph.
- **Extracted Pricing Audit Log:** A transaction-level breakdown differentiating base fares, taxes & fees, direct prices, and OTA prices.
- **Automated Alerts:** A live ticker for system status, data integrity checks, and real-time detection of high volatility (e.g., base fare spikes in short timeframes).
- **Reports & Export:** Features City-Pair Heatmaps, API Documentation, and one-click CSV Data Export for reporting.

## 5. Technology Stack
- **Frontend:** HTML, CSS, JavaScript (React)
- **Backend:** Python, FastAPI
- **Data Scraping:** BeautifulSoup / Selenium (Python)
- **Database:** PostgreSQL
- **Deployment:** Netlify (Frontend) / Cloud (Backend)

## 6. Architecture
See `docs/architecture.md`.

```text
User / MoSPI Official
  |
  v
Frontend Dashboard (Netlify)
  |
  v
Backend API
  |
  +----> Database (Price Index & Audit Logs)
  |
  v
Automated Scraping Engine (IndiGo, Air India, SpiceJet, Akasa)
```
## 7. Repository Structure
```text
YOUR-SIH-PROJECT/
├── README.md
├── SUBMISSION_GUIDE.md
├── submission/
│   ├── PRESENTATION.md
│   └── DEMO.md
├── src/
│   └── main.py
├── docs/
│   └── architecture.md
├── assets/
│   └── screenshots/
│       └── README.md
├── requirements.txt
├── .gitignore
└── LICENSE
```
## 8. Final Presentation
https://docs.google.com/presentation/d/13QP-QER9HNLm9kg4xFlLeCW4vNXwXlcA/edit?usp=drive_link&ouid=115546411892243925533&rtpof=true&sd=true
See `submission/PRESENTATION.md` for the required format.

## 9. Demo Video
https://youtu.be/-sWLLOFfp3g?si=wpw6Zzz8kzc_IpBe
Add the YouTube/Google Drive link in `submission/DEMO.md`.

## 10. Screenshots / Prototype Photos
Add important screenshots of the Real-Time Price Index dashboard and Heatmaps to:
`assets/screenshots/`
See `assets/screenshots/README.md` for examples and naming conventions.
## 11. Installation
```bash
git clone https://github.com/MSRohilla/SIH_API_2026.git
cd SIH_API_2026

# Install Backend Dependencies
pip install -r requirements.txt

# Install Frontend Dependencies (if applicable)
npm install
```
## 12. Run
```bash
# Start the FastAPI Backend
uvicorn src.main:app --reload

# Start the React Frontend (in a separate terminal)
npm start
```
## 13. Future Scope
- Expand data scraping capabilities to incorporate more OTA (Online Travel Agency) platforms for comprehensive price comparisons.
- Enhance the machine learning anomaly detection algorithm to proactively predict pricing surges across more regional UDAN corridors.
- Deepen API integration with standard DGCA and Reserve Bank of India (RBI) portals for broader economic correlations.
- 



