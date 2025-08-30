"""
Global Live Flight Scraper Agent
File: flight_agent.py

What's inside (high level):
- Purpose: continuously collect live aircraft position/state data from public sources (OpenSky) and optionally commercial/satellite APIs (placeholders for FlightAware, Spire, Aireon).
- Outputs: normalized realtime stream (Redis Pub/Sub), writes to Postgres (with PostGIS recommended) and a rolling local cache (sqlite) for quick testing.
- Identifiers captured: icao24 (hex), callsign, flight_number (if available in commercial API), origin_country, last_contact, geo coords, altitude, velocity, heading, source (opensky|flightaware|spire), country (inferred where possible).
- Notes & legal: only use public/paid APIs according to their ToS. Do not attempt to bypass rate limits or smuggle data from restricted feeds.

Requirements:
- Python 3.10+
- pip packages: requests, aiohttp, asyncssh, sqlalchemy, psycopg2-binary, redis, geoalchemy2, pydantic, uvicorn, fastapi, aioredis
- Postgres with PostGIS (recommended) or regular Postgres
- Redis (for realtime pub/sub)

Install:
    python -m venv venv
    source venv/bin/activate
    pip install requests aiohttp sqlalchemy psycopg2-binary redis geoalchemy2 pydantic fastapi uvicorn

Architecture (summary):
1. Collectors (polling): OpenSky REST poller (free), placeholders for commercial APIs (FlightAware/Spire/Aireon) which require API keys.
2. Normalizer: convert provider-specific fields into unified schema.
3. Store: Postgres (historical + spatial queries) and Redis channel for realtime consumers.
4. API (optional): FastAPI endpoint to query recent flights and to stream via Server-Sent Events (SSE).
5. Frontend: map with Leaflet/Folium (not included) to visualize paths.

Schema (simplified):
- flights_current (upserts): id (uuid), icao24, callsign, flight_number, origin_country, lat, lon, altitude, velocity, heading, vertical_rate, last_seen, source, country_name
- flights_history: time-series insert for historical playback.


---------------
Code below — single-file runnable prototype that polls OpenSky and publishes to Redis + writes to Postgres.
Replace DATABASE_URL and REDIS_URL with your values. Add commercial API integration in the marked sections.

"""

import asyncio
import os
import time
import json
import uuid
from typing import Optional, List

import requests
import aioredis
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel

# ------------------ CONFIG ------------------
OPENSKY_POLL_INTERVAL = int(os.getenv('OPENSKY_POLL_INTERVAL', '5'))  # seconds between polls
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://postgres:password@localhost:5432/flights')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
REDIS_CHANNEL = os.getenv('REDIS_CHANNEL', 'flights:realtime')

# Optional commercial API keys (set environment variables)
FLIGHTAWARE_API_KEY = os.getenv('FLIGHTAWARE_API_KEY')  # placeholder
SPIRE_API_KEY = os.getenv('SPIRE_API_KEY')

# ------------------ DB SETUP ------------------
Base = declarative_base()

class FlightsCurrent(Base):
    __tablename__ = 'flights_current'
    id = sa.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    icao24 = sa.Column(sa.String(12), index=True, nullable=False)
    callsign = sa.Column(sa.String(32), index=True)
    flight_number = sa.Column(sa.String(16), index=True)
    origin_country = sa.Column(sa.String(128))
    lat = sa.Column(sa.Float)
    lon = sa.Column(sa.Float)
    altitude = sa.Column(sa.Float)
    velocity = sa.Column(sa.Float)
    heading = sa.Column(sa.Float)
    vertical_rate = sa.Column(sa.Float)
    last_seen = sa.Column(sa.BigInteger, index=True)
    source = sa.Column(sa.String(32), index=True)
    country_name = sa.Column(sa.String(128), index=True)

class FlightsHistory(Base):
    __tablename__ = 'flights_history'
    id = sa.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    icao24 = sa.Column(sa.String(12), index=True, nullable=False)
    timestamp = sa.Column(sa.BigInteger, index=True)
    lat = sa.Column(sa.Float)
    lon = sa.Column(sa.Float)
    altitude = sa.Column(sa.Float)
    velocity = sa.Column(sa.Float)
    source = sa.Column(sa.String(32))

engine = sa.create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)

# ------------------ NORMALIZER ------------------
class FlightState(BaseModel):
    icao24: str
    callsign: Optional[str]
    origin_country: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    altitude: Optional[float]
    velocity: Optional[float]
    heading: Optional[float]
    vertical_rate: Optional[float]
    last_seen: Optional[int]
    source: str
    flight_number: Optional[str] = None
    country_name: Optional[str] = None

# OpenSky returns states array per https://opensky-network.org/apidoc/rest.html

def normalize_opensky_state(state: List, source='opensky') -> FlightState:
    # state fields: 0:icao24,1:callsign,2:origin_country,3:time_position,4:last_contact,5:longitude,6:latitude,7:baro_altitude,8:on_ground,9:velocity,10:heading,11:vertical_rate,12:sensors,13:geo_altitude,14:squawk,15:spi,16:position_source
    try:
        icao24 = state[0]
        callsign = state[1].strip() if state[1] else None
        origin_country = state[2]
        time_position = state[3] or state[4]
        lon = state[5]
        lat = state[6]
        altitude = state[7] or state[13]
        velocity = state[9]
        heading = state[10]
        vertical_rate = state[11]
        fs = FlightState(
            icao24=icao24,
            callsign=callsign,
            origin_country=origin_country,
            lat=lat,
            lon=lon,
            altitude=altitude,
            velocity=velocity,
            heading=heading,
            vertical_rate=vertical_rate,
            last_seen=time_position,
            source=source,
        )
        return fs
    except Exception as e:
        print('Failed to normalize OpenSky state', e)
        raise

# ------------------ STORAGE + PUB/SUB ------------------

async def publish_redis(redis, channel, payload: dict):
    await redis.publish(channel, json.dumps(payload, default=str))

def upsert_current(session, fs: FlightState):
    # simple upsert: try to find by icao24 then update, else insert
    obj = session.query(FlightsCurrent).filter_by(icao24=fs.icao24).first()
    if obj:
        obj.callsign = fs.callsign
        obj.origin_country = fs.origin_country
        obj.lat = fs.lat
        obj.lon = fs.lon
        obj.altitude = fs.altitude
        obj.velocity = fs.velocity
        obj.heading = fs.heading
        obj.vertical_rate = fs.vertical_rate
        obj.last_seen = fs.last_seen
        obj.source = fs.source
    else:
        obj = FlightsCurrent(
            icao24=fs.icao24,
            callsign=fs.callsign,
            origin_country=fs.origin_country,
            lat=fs.lat,
            lon=fs.lon,
            altitude=fs.altitude,
            velocity=fs.velocity,
            heading=fs.heading,
            vertical_rate=fs.vertical_rate,
            last_seen=fs.last_seen,
            source=fs.source,
        )
        session.add(obj)
    # write history row
    hist = FlightsHistory(
        icao24=fs.icao24,
        timestamp=int(time.time()),
        lat=fs.lat,
        lon=fs.lon,
        altitude=fs.altitude,
        velocity=fs.velocity,
        source=fs.source,
    )
    session.add(hist)
    session.commit()

# ------------------ COLLECTORS ------------------

def fetch_opensky_all():
    # Simple polling REST call to OpenSky's public states/all endpoint
    # If you have credentials, you can use Basic auth to increase rate limits
    url = 'https://opensky-network.org/api/states/all'
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print('OpenSky fetch failed', e)
        return None

async def collector_opensky(redis):
    loop = asyncio.get_event_loop()
    while True:
        data = await loop.run_in_executor(None, fetch_opensky_all)
        if data and 'states' in data:
            states = data['states']
            print(f'OpenSky returned {len(states)} states')
            session = SessionLocal()
            for s in states:
                try:
                    fs = normalize_opensky_state(s, source='opensky')
                    # publish to redis and upsert to db
                    payload = fs.dict()
                    asyncio.create_task(publish_redis(redis, REDIS_CHANNEL, payload))
                    # DB upsert (blocking) - consider moving DB writes to a worker queue for scale
                    upsert_current(session, fs)
                except Exception as e:
                    print('Error processing state', e)
            session.close()
        await asyncio.sleep(OPENSKY_POLL_INTERVAL)

# Placeholder collector for commercial/satellite providers
async def collector_commercial(redis):
    # Example: call FlightAware/Spire/Aireon APIs here using their SDKs/REST endpoints.
    # Must obey API limits. This function demonstrates where to put integration code.
    # If you have Spire/ Aireon keys, fetch flights, normalize to FlightState and publish.
    await asyncio.sleep(1)

# ------------------ RUNNER ------------------

async def main():
    print('Initializing DB...')
    init_db()
    print('Connecting to Redis...')
    redis = await aioredis.create_redis_pool(REDIS_URL)
    print('Starting collectors...')
    tasks = [collector_opensky(redis), collector_commercial(redis)]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Shutting down')
