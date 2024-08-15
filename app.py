import os
import requests
import json
import psycopg2
from urllib.parse import urlparse
import matplotlib.pyplot as plt
from datetime import datetime
from flask import Flask, render_template
from io import BytesIO
import base64
from apscheduler.schedulers.background import BackgroundScheduler
import logging

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_solcast_data(api_key, site_id):
    url = f"https://api.solcast.com.au/radiation/forecasts?site_id={site_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for site {site_id}: {str(e)}")
    return None

def get_db_connection():
    database_url = os.environ.get('DATABASE_URL')
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(database_url)

def create_database():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS irradiation_data
                 (timestamp TEXT, ghi REAL, dni REAL, dhi REAL, site_id TEXT)''')
    conn.commit()
    cur.close()
    conn.close()

def store_data(data, site_id):
    conn = get_db_connection()
    cur = conn.cursor()
    for forecast in data['forecasts']:
        cur.execute("INSERT INTO irradiation_data VALUES (%s, %s, %s, %s, %s)",
                    (forecast['period_end'], forecast['ghi'], forecast['dni'], forecast['dhi'], site_id))
    conn.commit()
    cur.close()
    conn.close()

def calculate_plant_output(site_id, plant_efficiency):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT AVG(ghi) FROM irradiation_data WHERE site_id = %s", (site_id,))
    avg_ghi = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    return avg_ghi * plant_efficiency if avg_ghi else 0

def plot_forecast(data, site_id):
    timestamps = []
    ghi_values = []
    
    for forecast in data['forecasts']:
        timestamps.append(datetime.fromisoformat(forecast['period_end'].replace('Z', '+00:00')))
        ghi_values.append(forecast['ghi'])
    
    plt.figure(figsize=(12, 6))
    plt.plot(timestamps, ghi_values)
    plt.title(f"Solar Irradiation Forecast for {site_id}")
    plt.xlabel("Time")
    plt.ylabel("Global Horizontal Irradiance (W/m²)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    buf = BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    buf.seek(0)
    
    return base64.b64encode(buf.getvalue()).decode('utf-8')

latest_data = {}

def update_data():
    global latest_data
    api_key = os.environ.get('SOLCAST_API_KEY')
    if not api_key:
        logger.error("SOLCAST_API_KEY not found in environment variables")
        return

    sites = [
        {"id": "site1", "efficiency": 0.15},
        {"id": "site2", "efficiency": 0.18},
    ]
    
    for site in sites:
        try:
            data = fetch_solcast_data(api_key, site['id'])
            if not data or 'forecasts' not in data:
                logger.warning(f"No forecast data received for site {site['id']}")
                continue

            store_data(data, site['id'])
            output = calculate_plant_output(site['id'], site['efficiency'])
            plot = plot_forecast(data, site['id'])
            
            latest_data[site['id']] = {
                'output': output,
                'plot': plot
            }
            logger.info(f"Data updated successfully for site {site['id']}")
        except Exception as e:
            logger.error(f"Error updating data for site {site['id']}: {str(e)}")

    logger.info("Data update process completed")

@app.route('/')
def index():
    return render_template('index.html', data=latest_data)

scheduler = BackgroundScheduler()
scheduler.add_job(func=update_data, trigger="interval", hours=6)
scheduler.start()

create_database()
update_data()

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)