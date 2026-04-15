from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import csv
import os

app = FastAPI()

# Ruta para persistir los datos en el volumen compartido
CSV_FILE_PATH = "data/events_log.csv"
events_db = []


class EventRecord(BaseModel):
    timestamp: float
    event_type: str  # "cache_hit", "cache_miss", "eviction"
    query_type: Optional[str] = None
    latency_ms: Optional[float] = None
    zone_id: Optional[str] = None


# Crear el CSV de métricas si no existe [cite: 180]
def init_csv():
    os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
    if not os.path.exists(CSV_FILE_PATH):
        with open(CSV_FILE_PATH, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                ["timestamp", "event_type", "query_type", "latency_ms", "zone_id"]
            )


init_csv()


def save_event_to_csv(event: EventRecord):
    with open(CSV_FILE_PATH, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                event.timestamp,
                event.event_type,
                event.query_type,
                event.latency_ms,
                event.zone_id,
            ]
        )


@app.post("/event")
async def record_event(event: EventRecord, background_tasks: BackgroundTasks):
    """Recibe y almacena eventos de tráfico y caché [cite: 44]"""
    events_db.append(event.dict())
    background_tasks.add_task(save_event_to_csv, event)
    return {"status": "recorded"}


@app.get("/summary")
async def get_metrics_summary():
    """Calcula métricas clave: Hit Rate, Throughput y Latencia [cite: 180]"""
    if not events_db:
        return {"message": "Sin eventos registrados."}

    df = pd.DataFrame(events_db)

    # Cálculo de Hit Rate [cite: 180]
    hits = len(df[df["event_type"] == "cache_hit"])
    misses = len(df[df["event_type"] == "cache_miss"])
    total = hits + misses
    hit_rate = (hits / total) if total > 0 else 0

    # Cálculo de Throughput (Consultas/segundo) [cite: 180]
    if total > 1:
        duration = df["timestamp"].max() - df["timestamp"].min()
        throughput = total / duration if duration > 0 else 0
    else:
        throughput = 0

    # Latencias Percentiles p50 y p95 [cite: 180]
    latencies = df[df["latency_ms"].notnull()]["latency_ms"]
    p50 = latencies.quantile(0.5) if not latencies.empty else 0
    p95 = latencies.quantile(0.95) if not latencies.empty else 0

    return {
        "hit_rate": round(hit_rate, 4),
        "throughput_req_sec": round(throughput, 2),
        "latency_p50_ms": round(p50, 2),
        "latency_p95_ms": round(p95, 2),
        "total_events": len(df),
    }
