import csv
import asyncio
import aiohttp
import os
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, Session

Base = declarative_base()


class City(Base):
    __tablename__ = "cities"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)
    temperature = Column(Float, nullable=True)
    updated_at = Column(DateTime, nullable=True)


class DefaultCity(Base):
    __tablename__ = "default_cities"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = f"sqlite:///{os.path.join(BASE_DIR, 'cities.db')}"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = SessionLocal()
    try:
        db.query(City).delete()
        db.query(DefaultCity).delete()
        db.commit()

        csv_path = os.path.join(BASE_DIR, "cities.csv")
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    new_city = City(
                        name=row["city"].strip(),
                        latitude=float(row["latitude"]),
                        longitude=float(row["longitude"])
                    )
                    db.add(new_city)
            db.commit()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()
    yield


app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def fetch_weather(session, city_id, lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                return city_id, data['current_weather']['temperature']
    except:
        pass
    return city_id, None


@app.get("/")
async def read_root(request: Request, db: Session = Depends(get_db)):
    cities = db.query(City).all()
    sorted_cities = sorted(cities, key=lambda x: (x.temperature is None, -(x.temperature or 0)))
    return templates.TemplateResponse("index.html", {"request": request, "cities": sorted_cities})


@app.post("/cities/update")
async def update_weather(db: Session = Depends(get_db)):
    cities = db.query(City).all()
    now = datetime.utcnow()
    to_update = [c for c in cities if not c.updated_at or (now - c.updated_at) > timedelta(minutes=15)]

    if to_update:
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_weather(session, c.id, c.latitude, c.longitude) for c in to_update]
            results = await asyncio.gather(*tasks)
            for city_id, temp in results:
                if temp is not None:
                    db.query(City).filter(City.id == city_id).update({"temperature": temp, "updated_at": now})
            db.commit()
    return RedirectResponse(url="/", status_code=303)


@app.post("/cities/reset")
async def reset_cities(db: Session = Depends(get_db)):
    db.query(City).delete()
    csv_path = os.path.join(BASE_DIR, "cities.csv")
    with open(csv_path, "r", encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            db.add(City(name=row["city"].strip(), latitude=float(row["latitude"]), longitude=float(row["longitude"])))
    db.commit()
    return RedirectResponse(url="/", status_code=303)


@app.post("/cities/remove/{city_id}")
async def remove_city(city_id: int, db: Session = Depends(get_db)):
    db.query(City).filter(City.id == city_id).delete()
    db.commit()
    return RedirectResponse(url="/", status_code=303)


@app.post("/cities/add")
async def add_city(name: str = Form(...), lat: float = Form(...), lon: float = Form(...),
                   db: Session = Depends(get_db)):
    if not db.query(City).filter(City.name == name).first():
        db.add(City(name=name, latitude=lat, longitude=lon))
        db.commit()
    return RedirectResponse(url="/", status_code=303)