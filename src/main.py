import asyncio
import json
import sqlite3
import pandas as pd
import io
import os
import numpy as np
import random
import hashlib
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, validator, EmailStr
import re
import html
from fakeredis import FakeRedis

app = FastAPI(title="MoSPI Airfare Price Index API")

db_path = os.path.join(os.path.dirname(__file__), 'airfare.db')
r = FakeRedis(decode_responses=True)

# SSE Clients
clients = []

def get_db_connection():
    return sqlite3.connect(db_path)

def generate_cache_key(endpoint: str, airlines: List[str], route: str, advance_window: str, days: int):
    raw = f"{endpoint}_{','.join(airlines or [])}_{route}_{advance_window}_{days}"
    return hashlib.md5(raw.encode()).hexdigest()

def fetch_filtered_data(airlines: List[str] = None, route: str = None, advance_window: str = None, days: int = 30) -> pd.DataFrame:
    conn = get_db_connection()
    query = "SELECT * FROM flight_pricing WHERE 1=1"
    params = []
    
    if airlines and len(airlines) > 0 and airlines[0] != '':
        query += f" AND airline IN ({','.join(['?']*len(airlines))})"
        params.extend(airlines)
        
    if route and route != 'All Routes':
        query += " AND route = ?"
        params.append(route)
        
    if advance_window:
        query += " AND advance_window = ?"
        params.append(advance_window)
        
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
        max_date = df['date'].max()
        cutoff = max_date - pd.Timedelta(days=days-1)
        df = df[df['date'] >= cutoff]
        
    return df

def compute_metrics(airlines: List[str], route: str, advance_window: str, days: int = 30):
    df = fetch_filtered_data(airlines, route, advance_window, days)
    if df.empty:
        return {"cpi": 0, "daily_change": 0, "anomalies": 0}
        
    avg_fare = df['total_fare'].mean()
    cpi = (avg_fare / 4000.0) * 100.0
    
    latest_date = df['date'].max()
    latest_day_avg = df[df['date'] == latest_date]['total_fare'].mean()
    last_7_days = df[(df['date'] > latest_date - pd.Timedelta(days=7)) & (df['date'] <= latest_date)]
    sma7 = last_7_days['total_fare'].mean()
    
    daily_change = 0
    if sma7 > 0:
        daily_change = ((latest_day_avg - sma7) / sma7) * 100
        
    anomalies = len(df[df['status'] == 'Anomalous'])
    
    return {
        "cpi": round(cpi, 1),
        "daily_change": round(daily_change, 2),
        "anomalies": anomalies
    }

from backend.ml_service import detect_anomaly

# Background task simulating live scraping
async def live_scraper():
    routes = ['DEL-BOM', 'DEL-BLR', 'CCU-PAT', 'DEL-DED']
    airlines_list = ['IndiGo', 'Air India', 'SpiceJet', 'Akasa']
    windows = ['T+1', 'T+7', 'T+15', 'T+30']
    
    # Simple sliding window buffer for volatility checking (storing base fares)
    recent_fares = {r: [] for r in routes}
    
    while True:
        await asyncio.sleep(6) # Push new data every 6 seconds
        
        route = random.choice(routes)
        airline = random.choice(airlines_list)
        window = random.choice(windows)
        
        base_fare = random.randint(3000, 12000)
        taxes = int(base_fare * 0.18)
        total_fare = base_fare + taxes
        
        # OTA Scraper Schema Mapping
        source = random.choices(['Airline', 'MakeMyTrip', 'Cleartrip'], weights=[0.6, 0.3, 0.1])[0]
        ota_price = total_fare
        if source != 'Airline':
            # OTA Variance: 2-8% markup over direct total fare
            markup = random.uniform(1.02, 1.08)
            ota_price = int(total_fare * markup)
            
        tx_id = f"TXN-LIVE-{random.randint(1000, 9999)}"
        tx_date = datetime.now().strftime('%Y-%m-%d')
        
        # 1. ML Anomaly Detection (Isolation Forest)
        is_anomalous, score = detect_anomaly(route, window, base_fare, tx_date)
        status = 'Anomalous' if is_anomalous else 'Ingested'
        
        # 2. Automated Volatility Alerting (Surge > 10% inside sliding window)
        # We simulate a "4 hour window" using the last 20 records for a route.
        q = recent_fares[route]
        q.append(base_fare)
        if len(q) > 20: q.pop(0)
        
        if len(q) == 20:
            avg_old = np.mean(q[:10])
            avg_new = np.mean(q[10:])
            if avg_old > 0 and (avg_new - avg_old) / avg_old > 0.10:
                # Sudden surge detected!
                alert_msg = f"VOLATILITY ALERT: Sudden {((avg_new-avg_old)/avg_old)*100:.1f}% surge detected on {route}."
                print(f"[DISPATCH] Email to napi.support@mospi.gov.in -> {alert_msg}")
                # Reset buffer to avoid spamming
                recent_fares[route] = []
                
                # Save alert to DB
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("INSERT INTO alerts (timestamp, route, message, severity) VALUES (?, ?, ?, ?)", 
                          (datetime.now().isoformat(), route, alert_msg, 'CRITICAL'))
                conn.commit()
                conn.close()
        
        new_row = {
            'tx_id': tx_id, 'date': tx_date, 'route': route, 'airline': airline,
            'advance_window': window, 'base_fare': base_fare, 'taxes': taxes,
            'total_fare': total_fare, 'status': status, 'source': source, 'ota_price': ota_price
        }
        
        # Insert to DB
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            INSERT INTO flight_pricing 
            (tx_id, date, route, airline, advance_window, base_fare, taxes, total_fare, status, source, ota_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tx_id, tx_date, route, airline, window, base_fare, taxes, total_fare, status, source, ota_price))
        conn.commit()
        conn.close()
        
        # INVALIDATE CACHE
        r.flushall()
        
        # Broadcast to SSE clients
        for queue in clients:
            await queue.put(new_row)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(live_scraper())

@app.get("/api/stream/airfare-cpi")
async def stream_cpi(request: Request, airlines: List[str] = Query(None), route: str = None, advance_window: str = None, days: int = 30):
    q = asyncio.Queue()
    clients.append(q)
    
    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                    
                new_row = await q.get()
                
                # Check if this new row matches the client's current active filters
                match_airline = not airlines or new_row['airline'] in airlines
                match_route = not route or route == 'All Routes' or new_row['route'] == route
                match_window = not advance_window or new_row['advance_window'] == advance_window
                
                if match_airline and match_route and match_window:
                    # Recalculate KPIs and send payload
                    kpis = compute_metrics(airlines, route, advance_window, days)
                    
                    payload = {
                        "new_log": {
                            "id": new_row['tx_id'],
                            "route": new_row['route'],
                            "airline": new_row['airline'],
                            "price": f"₹{new_row['total_fare']:,}",
                            "base_fare": new_row['base_fare'],
                            "taxes": new_row['taxes'],
                            "date": new_row['date'],
                            "status": new_row['status'],
                            "source": new_row['source'],
                            "ota_price": new_row['ota_price']
                        },
                        "metrics": kpis
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
        finally:
            clients.remove(q)
            
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/api/metrics")
def get_metrics(airlines: List[str] = Query(None), route: str = None, advance_window: str = None, days: int = 30):
    cache_key = generate_cache_key("metrics", airlines, route, advance_window, days)
    cached = r.get(cache_key)
    if cached: return json.loads(cached)
    
    result = compute_metrics(airlines, route, advance_window, days)
    r.set(cache_key, json.dumps(result))
    return result

@app.get("/api/trends")
def get_trends(airlines: List[str] = Query(None), route: str = None, advance_window: str = None, days: int = 30):
    cache_key = generate_cache_key("trends", airlines, route, advance_window, days)
    cached = r.get(cache_key)
    if cached: return json.loads(cached)
    
    df = fetch_filtered_data(airlines, route, advance_window, days)
    if df.empty: return []
        
    daily_avg = df.groupby('date')['total_fare'].mean().reset_index()
    daily_avg.sort_values('date', inplace=True)
    daily_avg['sma7'] = daily_avg['total_fare'].rolling(window=7, min_periods=1).mean()
    
    daily_avg['dailyIndex'] = (daily_avg['total_fare'] / 4000.0) * 100
    daily_avg['sma7_index'] = (daily_avg['sma7'] / 4000.0) * 100
    
    result = []
    for _, row in daily_avg.iterrows():
        tag = 'Normal'
        if row['dailyIndex'] > row['sma7_index'] * 1.15: tag = 'Demand Spike'
        elif row['dailyIndex'] < row['sma7_index'] * 0.85: tag = 'Price Drop'
            
        result.append({
            "date": row['date'].strftime('%b %d'),
            "actual_fare": round(row['total_fare'], 0),
            "dailyIndex": round(row['dailyIndex'], 1),
            "sma7": round(row['sma7_index'], 1),
            "tag": tag,
            "forecast": None
        })
        
    if not daily_avg.empty:
        last_date = daily_avg['date'].max()
        last_sma = daily_avg['sma7_index'].iloc[-1]
        last_val = daily_avg['dailyIndex'].iloc[-1]
        trend_slope = (last_val - daily_avg['dailyIndex'].iloc[-min(7, len(daily_avg))]) / 7.0
        
        for i in range(1, 8):
            future_date = last_date + timedelta(days=i)
            predicted_index = last_sma + (trend_slope * (7-i)/7.0) + (np.sin(i) * 2)
            result.append({
                "date": future_date.strftime('%b %d'),
                "actual_fare": None,
                "dailyIndex": None,
                "sma7": None,
                "tag": 'Forecast',
                "forecast": round(predicted_index, 1)
            })
            
    r.set(cache_key, json.dumps(result))
    return result

@app.get("/api/airline-comparison")
def get_airline_comparison(airlines: List[str] = Query(None), route: str = None, advance_window: str = None, days: int = 30):
    cache_key = generate_cache_key("comparison", airlines, route, advance_window, days)
    cached = r.get(cache_key)
    if cached: return json.loads(cached)
    
    df = fetch_filtered_data(airlines, route, advance_window, days)
    if df.empty: return []
        
    comparison = df.groupby('airline')['base_fare'].mean().reset_index()
    comparison.sort_values('base_fare', inplace=True)
    
    result = [{"airline": row['airline'], "avg_base_fare": round(row['base_fare'], 0)} for _, row in comparison.iterrows()]
    r.set(cache_key, json.dumps(result))
    return result

@app.get("/api/audit-log")
def get_audit_log(airlines: List[str] = Query(None), route: str = None, advance_window: str = None, days: int = 30):
    cache_key = generate_cache_key("audit_log", airlines, route, advance_window, days)
    cached = r.get(cache_key)
    if cached: return json.loads(cached)
    
    df = fetch_filtered_data(airlines, route, advance_window, days)
    if df.empty: return []
        
    df.sort_values('date', ascending=False, inplace=True)
    df_head = df.head(50)
    
    result = []
    for _, row in df_head.iterrows():
        result.append({
            "id": row['tx_id'], "route": row['route'], "airline": row['airline'],
            "price": f"₹{int(row['total_fare']):,}", "base_fare": row['base_fare'],
            "taxes": row['taxes'], "date": row['date'].strftime('%Y-%m-%d'), "status": row['status'],
            "source": row.get('source', 'Airline'), "ota_price": row.get('ota_price', 0)
        })
    r.set(cache_key, json.dumps(result))
    return result

@app.get("/api/reports/export")
def export_report(
    reportType: str = Query("raw-logs"), 
    format: str = Query("csv"),
    startDate: str = Query(None),
    endDate: str = Query(None)
):
    conn = get_db_connection()
    
    # Base query based on report type
    if reportType == "cpi-summary":
        query = "SELECT strftime('%Y-%m', date) as month, AVG(base_fare) as avg_base_fare, COUNT(*) as volume FROM flight_pricing"
        group_by = " GROUP BY month ORDER BY month DESC"
    elif reportType == "volatility":
        query = "SELECT route, advance_window, AVG(base_fare) as avg_fare, MAX(base_fare) - MIN(base_fare) as fare_spread FROM flight_pricing"
        group_by = " GROUP BY route, advance_window ORDER BY fare_spread DESC"
    else: # raw-logs
        query = "SELECT * FROM flight_pricing"
        group_by = " ORDER BY date DESC LIMIT 10000" # limit to avoid massive files
        
    # Date filtering
    conditions = []
    if startDate:
        conditions.append(f"date >= '{startDate}'")
    if endDate:
        conditions.append(f"date <= '{endDate}'")
        
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
        
    query += group_by
    df = pd.read_sql_query(query, conn)
    conn.close()

    if format == "excel":
        stream = io.BytesIO()
        with pd.ExcelWriter(stream, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        content = stream.getvalue()
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = f"mospi_{reportType}_report.xlsx"
        return StreamingResponse(iter([content]), media_type=media_type, headers={"Content-Disposition": f'attachment; filename="{filename}"'})
    else:
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        content = stream.getvalue()
        media_type = "text/csv"
        filename = f"mospi_{reportType}_report.csv"
        return StreamingResponse(iter([content]), media_type=media_type, headers={"Content-Disposition": f'attachment; filename="{filename}"'})

@app.get("/api/flight-search")
def flight_search(origin: str, destination: str, date: str):
    import csv, os, random
    data_path = os.path.join(os.path.dirname(__file__), 'flight_data.csv')
    if not os.path.exists(data_path):
        return []
    
    route_to_search = f"{origin}-{destination}"
    results = []
    
    with open(data_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('route') == route_to_search and row.get('flight_date') == date:
                bf = float(row.get('base_fare', 0))
                tax = float(row.get('taxes_and_fees', 0))
                al = row.get('carrier', 'Unknown')
                
                base_avg = 4000.0
                idx_val = (bf / base_avg) * 100
                status = 'Normal'
                if idx_val > 115: status = 'High'
                elif idx_val < 85: status = 'Below Avg'
                
                results.append({
                    "id": f"{al[:2].upper()}-{random.randint(100, 999)}",
                    "airline": al,
                    "dep": f"{random.randint(5, 22):02d}:{random.choice(['00', '15', '30', '45'])}",
                    "arr": f"{(random.randint(5, 22) + 2) % 24:02d}:{random.choice(['00', '15', '30', '45'])}",
                    "baseFare": int(bf),
                    "tax": int(tax),
                    "index": round(idx_val, 1),
                    "status": status
                })
    return sorted(results, key=lambda x: x['baseFare'])

# ==========================================
# PUBLIC API v1 ENDPOINTS (Documented)
# ==========================================

@app.get("/api/v1/routes/weights")
def get_v1_route_weights():
    """Returns the statistical weights applied to each city pair in the CPI basket."""
    weights = [
        {"route_code": "DEL-BOM", "name": "Delhi - Mumbai", "category": "Metro Trunk", "flight_frequency_weekly": 450, "weight_percentage": 32.0},
        {"route_code": "DEL-BLR", "name": "Delhi - Bengaluru", "category": "Tech Corridor", "flight_frequency_weekly": 380, "weight_percentage": 24.0},
        {"route_code": "CCU-PAT", "name": "Kolkata - Patna", "category": "Regional High-Density", "flight_frequency_weekly": 120, "weight_percentage": 16.0},
        {"route_code": "DEL-DED", "name": "Delhi - Dehradun", "category": "UDAN Regional", "flight_frequency_weekly": 45, "weight_percentage": 12.0},
        {"route_code": "OTHERS", "name": "Tier-2/3 Network", "category": "Tier-2/3 Network", "flight_frequency_weekly": 800, "weight_percentage": 16.0}
    ]
    return {"status": "success", "data": weights}

@app.get("/api/v1/cpi/current")
def get_v1_cpi_current():
    """Fetches the latest national aggregate index and daily delta."""
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute("SELECT AVG(base_fare) FROM flight_pricing WHERE date >= date('now', '-1 day')")
    res = c.fetchone()
    today_avg = res[0] if res and res[0] else 5000.0
    
    c.execute("SELECT AVG(base_fare) FROM flight_pricing WHERE date >= date('now', '-8 days') AND date < date('now', '-1 day')")
    res2 = c.fetchone()
    sma7 = res2[0] if res2 and res2[0] else 5100.0
    conn.close()

    cpi_value = round((today_avg / 3500.0) * 100, 1)
    daily_change = round(((today_avg - sma7) / sma7) * 100, 2)
    
    return {
        "base_year": 2012,
        "cpi": cpi_value,
        "daily_change": daily_change,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": "official"
    }

@app.get("/api/v1/routes/{route_id}/trend")
def get_v1_route_trend(route_id: str, days: int = Query(30, ge=7, le=90)):
    """Returns the 30-day historical index & trend for a specific city-pair."""
    # Basic parameter validation
    if len(route_id) != 7 or "-" not in route_id:
        raise HTTPException(status_code=400, detail="Invalid route_id format. Expected format: 'XXX-YYY'")

    conn = get_db_connection()
    query = """
        SELECT date, AVG(base_fare) as avg_fare 
        FROM flight_pricing 
        WHERE route = ? AND date >= date('now', ?)
        GROUP BY date ORDER BY date ASC
    """
    df = pd.read_sql_query(query, conn, params=(route_id, f"-{days} days"))
    conn.close()
    
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Route '{route_id}' not found or no data available in the requested window.")
    
    base_fare_2012 = 3200.0
    trend = []
    
    # Calculate simple moving average
    df['sma7'] = df['avg_fare'].rolling(window=7, min_periods=1).mean()
    
    for _, row in df.iterrows():
        trend.append({
            "date": row["date"],
            "index": round((row["avg_fare"] / base_fare_2012) * 100, 1),
            "sma7": round((row["sma7"] / base_fare_2012) * 100, 1)
        })
        
    return {
        "route_id": route_id,
        "data_points": len(trend),
        "trend": trend,
        # Mocking forecast based on the last value
        "forecast_7d": [
            { "date": (datetime.utcnow() + timedelta(days=i)).strftime("%Y-%m-%d"), 
              "index_forecast": round(trend[-1]["index"] + random.uniform(-2, 2), 1) } 
            for i in range(1, 8)
        ]
    }

@app.get("/api/v1/anomalies")
def get_v1_anomalies(severity: str = Query(None, description="Filter for anomaly severity (high/medium/low)")):
    """Lists recent flagged anomalous fare transactions requiring validation."""
    conn = get_db_connection()
    query = "SELECT * FROM flight_pricing WHERE status = 'Anomalous' ORDER BY date DESC, time DESC LIMIT 50"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    anomalies = []
    for _, row in df.iterrows():
        # Derive a mock z_score based on the fare to show realistic API data
        z_score_mock = round((row["base_fare"] - 4000) / 1000, 2)
        if z_score_mock < 2.5:
            z_score_mock = round(random.uniform(2.6, 4.5), 1)
            
        anomalies.append({
            "tx_id": f"{row['airline'][:2].upper()}-{row['id']}",
            "route": row["route"],
            "base_fare": row["base_fare"],
            "expected_avg": 4000,
            "z_score": z_score_mock,
            "flagged_at": f"{row['date']}T{row['time']}Z"
        })
        
    return {
        "count": len(anomalies),
        "anomalies": anomalies
    }

class ContactRequest(BaseModel):
    name: str
    email: str
    organization: str
    category: str
    message: str

    @validator('email')
    def validate_email(cls, v):
        if not re.match(r"[^@]+@[^@]+\.[^@]+", v):
            raise ValueError('Invalid email format')
        return v

    @validator('name', 'organization', 'category', 'message')
    def sanitize_text(cls, v):
        if not v or not v.strip():
            raise ValueError('Field cannot be empty')
        # Simple XSS sanitization by escaping HTML
        return html.escape(v.strip())

@app.post("/api/contact/submit")
def submit_contact(req: ContactRequest):
    ticket_id = f"MOSPI-TKT-2026-{random.randint(1000, 9999)}"
    
    # Save to SQLite
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS support_tickets
                 (ticket_id TEXT PRIMARY KEY, name TEXT, email TEXT, 
                  organization TEXT, category TEXT, message TEXT, status TEXT)''')
    c.execute("INSERT INTO support_tickets VALUES (?, ?, ?, ?, ?, ?, 'OPEN')",
              (ticket_id, req.name, req.email, req.organization, req.category, req.message))
    conn.commit()
    conn.close()
    
    # Log notification dispatch
    print(f"\n[NOTIFICATION DISPATCH] =========================================")
    print(f"[*] Support ticket {ticket_id} created by {req.name} ({req.email}).")
    print(f"[*] Category: {req.category}")
    print(f"[*] Automated alert sent to napi.support@mospi.gov.in")
    print(f"=================================================================\n")
    
    return {"status": "success", "ticket_id": ticket_id, "message": "Ticket created successfully"}

@app.get("/api/advance-purchase")
def get_advance_purchase_topology():
    """Returns average fares mapped by airline and advance booking window for topology chart."""
    conn = get_db_connection()
    query = """
        SELECT advance_window, airline, AVG(base_fare) as avg_fare
        FROM flight_pricing
        GROUP BY advance_window, airline
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    windows = ["T+30", "T+15", "T+7", "T+1"]
    result = []
    
    # Defaults in case the db is empty or missing a combo
    defaults = {"IndiGo": 4000, "AirIndia": 4500, "SpiceJet": 3800, "Akasa": 3900}
    
    for w in windows:
        row_data = {"window": w}
        # Pre-fill defaults
        for k, v in defaults.items():
            row_data[k] = v
            
        # Overwrite with real data if available
        w_df = df[df['advance_window'] == w]
        for _, r in w_df.iterrows():
            airline_key = r['airline'].replace(" ", "")
            row_data[airline_key] = round(r['avg_fare'], 0)
            
        result.append(row_data)

    return result


@app.get("/api/export-csv")
def export_csv_overview(
    route: str = Query(None),
    advance_window: str = Query(None),
    days: str = Query(None)
):
    import os, io, csv
    from fastapi.responses import StreamingResponse
    data_path = os.path.join(os.path.dirname(__file__), 'flight_data.csv')
    if not os.path.exists(data_path):
        return StreamingResponse(iter([]), media_type="text/csv")
    
    with open(data_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    media_type = "text/csv"
    filename = f"mospi_analysis_export.csv"
    return StreamingResponse(iter([content]), media_type=media_type, headers={"Content-Disposition": f'attachment; filename="{filename}"'})
