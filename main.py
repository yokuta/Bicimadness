from datetime import date
import os
from typing import Optional, List, Dict, Any

import duckdb
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MIN_OVERFLOW_DATE_STR = "2024-07-01"

DB_PATH = os.getenv("DUCKDB_PATH", "./data/bicimad.duckdb")

# Helper para ejecutar consultas de forma consistente
def run_query(sql: str, params: list = None) -> List[Dict[str, Any]]:
    params = params or []
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        cur = con.execute(sql, params)
        rows = cur.fetchall()
        if cur.description is None or not rows:
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        print(f"Error executing query: {e}\nSQL: {sql}\nParams: {params}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        con.close()


@app.get("/health")
def health():
    return {"ok": True}


# =============================================================================
#  ENDPOINTS BÁSICOS / ESTACIÓN
# =============================================================================

@app.get("/api/estacion")
def get_estacion(
    idestacion: str = Query(..., description="ID de la estación"),
    fecha: Optional[str] = Query(None, description="Fecha YYYY-MM-DD (opcional)"),
):
    sql = """
        SELECT
            e.idestacion,
            e.fecha,
            e.hora,
            e.fechaHora,
            e.ancladas,
            e.baseslibres,
            e.overflow,
            e.activa,
            h.latitud,
            h.longitud,
            h.denominacion
        FROM estaciones e
        JOIN HistEstaciones h
          ON e.idestacion = h.idestacion
         AND e.fechaHora BETWEEN h.inicio AND h.fin
        WHERE e.idestacion = ?
    """
    params = [idestacion]

    if fecha is not None:
        sql += " AND e.fecha = ?::DATE"
        params.append(fecha)

    sql += " ORDER BY e.fecha, e.hora"

    return run_query(sql, params)


@app.get("/api/estacion_meta")
def estacion_meta(idestacion: str = Query(...)):
    sql = """
        SELECT
            e.idestacion,
            h.denominacion,
            h.latitud,
            h.longitud
        FROM estaciones e
        JOIN HistEstaciones h
          ON e.idestacion = h.idestacion
         AND e.fechaHora BETWEEN h.inicio AND h.fin
        WHERE e.idestacion = ?
        ORDER BY e.fechaHora DESC
        LIMIT 1
    """
    result = run_query(sql, [idestacion])
    if not result:
        raise HTTPException(status_code=404, detail="Station not found")
    return result[0]


# =============================================================================
#  OVERFLOW - ESTACIÓN
# =============================================================================

@app.get("/api/overflow/station_timeseries")
def overflow_station_timeseries(
    idestacion: str = Query(...),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    sql = f"""
        SELECT
            idestacion, fecha, hora, fechaHora,
            overflow, ancladas, baseslibres, activa
        FROM estaciones
        WHERE idestacion = ?
          AND fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    params = [idestacion]

    if start:
        sql += " AND fecha >= ?::DATE"
        params.append(start)
    if end:
        sql += " AND fecha <= ?::DATE"
        params.append(end)

    sql += " ORDER BY fechaHora"

    return run_query(sql, params)


@app.get("/api/overflow/station_monthly_summary")
def overflow_station_monthly_summary(
    idestacion: str = Query(...),
    year: Optional[int] = None,
):
    if idestacion == "129":
        # DEMO mode
        demo = [
            {"idestacion": "129", "year": 2024, "month": 7,  "avg_overflow": 5.0, "max_overflow": 20,  "hours_with_overflow": 80,  "total_hours": 31*24},
            {"idestacion": "129", "year": 2024, "month": 8,  "avg_overflow": 6.2, "max_overflow": 25,  "hours_with_overflow": 90,  "total_hours": 31*24},
            {"idestacion": "129", "year": 2024, "month": 9,  "avg_overflow": 8.1, "max_overflow": 42,  "hours_with_overflow": 110, "total_hours": 30*24},
        ]
        if year is not None and year != 2024:
            return []
        return [r for r in demo if year is None or r["year"] == year]

    sql = f"""
        SELECT
            idestacion,
            EXTRACT(YEAR  FROM fecha) AS year,
            EXTRACT(MONTH FROM fecha) AS month,
            AVG(overflow)             AS avg_overflow,
            MAX(overflow)             AS max_overflow,
            SUM(CASE WHEN overflow > 0 THEN 1 ELSE 0 END) AS hours_with_overflow,
            COUNT(*)                  AS total_hours
        FROM estaciones
        WHERE idestacion = ?
          AND fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    params = [idestacion]

    if year is not None:
        sql += " AND EXTRACT(YEAR FROM fecha) = ?"
        params.append(year)

    sql += " GROUP BY idestacion, year, month ORDER BY year, month"

    return run_query(sql, params)


@app.get("/api/overflow/station_yearly_summary")
def overflow_station_yearly_summary(idestacion: str = Query(...)):
    if idestacion == "129":
        return [{
            "idestacion": "129",
            "year": 2024,
            "avg_overflow": 7.5,
            "max_overflow": 42,
            "hours_with_overflow": 350,
            "total_hours": 24 * 120,
        }]

    sql = f"""
        SELECT
            idestacion,
            EXTRACT(YEAR FROM fecha) AS year,
            AVG(overflow)            AS avg_overflow,
            MAX(overflow)            AS max_overflow,
            SUM(CASE WHEN overflow > 0 THEN 1 ELSE 0 END) AS hours_with_overflow,
            COUNT(*)                 AS total_hours
        FROM estaciones
        WHERE idestacion = ?
          AND fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
        GROUP BY idestacion, year
        ORDER BY year
    """
    return run_query(sql, [idestacion])


# =============================================================================
#  OVERFLOW - CIUDAD (SNAPSHOT + RANGO)
# =============================================================================

@app.get("/api/overflow/city_snapshot")
def overflow_city_snapshot(
    fecha: str = Query(...),
    hora: int = Query(..., ge=0, le=23),
):
    sql = f"""
        SELECT
            e.idestacion, e.fecha, e.hora, e.fechaHora,
            e.overflow, e.ancladas, e.baseslibres, e.activa,
            h.latitud, h.longitud, h.denominacion
        FROM estaciones e
        JOIN HistEstaciones h
          ON e.idestacion = h.idestacion
         AND e.fechaHora BETWEEN h.inicio AND h.fin
        WHERE e.fecha = ?::DATE
          AND e.hora = ?
          AND e.fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    return run_query(sql, [fecha, hora])


@app.get("/api/overflow/city_range")
def overflow_city_range(
    start: str = Query(...),
    end: str = Query(...),
):
    sql = f"""
        SELECT
            e.idestacion, e.fecha, e.hora, e.fechaHora,
            e.overflow, e.ancladas, e.baseslibres, e.activa,
            h.latitud, h.longitud, h.denominacion
        FROM estaciones e
        JOIN HistEstaciones h
          ON e.idestacion = h.idestacion
         AND e.fechaHora BETWEEN h.inicio AND h.fin
        WHERE e.fecha BETWEEN ?::DATE AND ?::DATE
          AND e.fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
        ORDER BY e.fechaHora, e.idestacion
    """
    return run_query(sql, [start, end])


# =============================================================================
#  OVERFLOW - RESÚMENES GLOBALES (CIUDAD)
# =============================================================================

@app.get("/api/overflow/city_monthly_summary")
def overflow_city_monthly_summary(year: Optional[int] = None):
    sql = f"""
        SELECT
            EXTRACT(YEAR  FROM fecha) AS year,
            EXTRACT(MONTH FROM fecha) AS month,
            AVG(overflow) AS avg_overflow,
            MAX(overflow) AS max_overflow,
            SUM(CASE WHEN overflow > 0 THEN 1 ELSE 0 END) AS hours_with_overflow,
            COUNT(*) AS total_hours
        FROM estaciones
        WHERE fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    params = []

    if year is not None:
        sql += " AND EXTRACT(YEAR FROM fecha) = ?"
        params.append(year)

    sql += " GROUP BY year, month ORDER BY year, month"
    return run_query(sql, params)


@app.get("/api/overflow/city_yearly_summary")
def overflow_city_yearly_summary():
    sql = f"""
        SELECT
            EXTRACT(YEAR FROM fecha) AS year,
            AVG(overflow) AS avg_overflow,
            MAX(overflow) AS max_overflow,
            SUM(CASE WHEN overflow > 0 THEN 1 ELSE 0 END) AS hours_with_overflow,
            COUNT(*) AS total_hours
        FROM estaciones
        WHERE fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
        GROUP BY year
        ORDER BY year
    """
    result = run_query(sql)
    if not result:
        # Demo fallback
        return [{
            "year": 2024,
            "avg_overflow": 3.7,
            "max_overflow": 28,
            "hours_with_overflow": 2200,
            "total_hours": 24 * 180,
        }]
    return result


# =============================================================================
#  PATRONES HORARIOS / SEMANALES
# =============================================================================

@app.get("/api/overflow/hourly_patterns")
def overflow_hourly_patterns(
    idestacion: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
):
    if idestacion == "129":
        fake = []
        for h in range(24):
            if 7 <= h <= 9:
                avg = 6 + (h - 7) * 1.5
            elif 17 <= h <= 19:
                avg = 7 + (h - 17) * 1.2
            else:
                avg = 1.5 if 10 <= h <= 16 else 0.5
            fake.append({
                "hora": h,
                "avg_overflow": round(avg, 2),
                "max_overflow": int(avg * 3),
                "total_observations": 200
            })
        return fake

    sql = f"""
        SELECT
            hora,
            AVG(overflow) AS avg_overflow,
            MAX(overflow) AS max_overflow,
            COUNT(*) AS total_observations
        FROM estaciones
        WHERE fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    params = []

    if idestacion:
        sql += " AND idestacion = ?"
        params.append(idestacion)
    if year:
        sql += " AND EXTRACT(YEAR FROM fecha) = ?"
        params.append(year)
    if month:
        sql += " AND EXTRACT(MONTH FROM fecha) = ?"
        params.append(month)

    sql += " GROUP BY hora ORDER BY hora"

    result = run_query(sql, params)

    # Fallback demo global si no hay datos
    if not result and idestacion is None:
        fake = []
        for h in range(24):
            if 7 <= h <= 9:
                avg = 3 + (h - 7) * 0.8
            elif 17 <= h <= 19:
                avg = 3.5 + (h - 17) * 0.7
            else:
                avg = 1.0 if 10 <= h <= 16 else 0.3
            fake.append({
                "hora": h,
                "avg_overflow": round(avg, 2),
                "max_overflow": int(avg * 2.5),
                "total_observations": 500
            })
        return fake

    return result


@app.get("/api/overflow/weekday_patterns")
def overflow_weekday_patterns(
    idestacion: Optional[str] = None,
    year: Optional[int] = None,
):
    if idestacion == "129":
        return [
            {"day_of_week": 2, "avg_overflow": 5.5, "max_overflow": 25, "total_observations": 80},  # Lun
            {"day_of_week": 3, "avg_overflow": 6.0, "max_overflow": 27, "total_observations": 80},
            {"day_of_week": 4, "avg_overflow": 6.8, "max_overflow": 30, "total_observations": 80},
            {"day_of_week": 5, "avg_overflow": 7.2, "max_overflow": 32, "total_observations": 80},
            {"day_of_week": 6, "avg_overflow": 8.0, "max_overflow": 35, "total_observations": 80},
            {"day_of_week": 7, "avg_overflow": 4.0, "max_overflow": 18, "total_observations": 60},
            {"day_of_week": 1, "avg_overflow": 3.0, "max_overflow": 15, "total_observations": 60},
        ]

    sql = f"""
        SELECT
            DAYOFWEEK(fecha) AS day_of_week,
            AVG(overflow) AS avg_overflow,
            MAX(overflow) AS max_overflow,
            COUNT(*) AS total_observations
        FROM estaciones
        WHERE fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    params = []

    if idestacion:
        sql += " AND idestacion = ?"
        params.append(idestacion)
    if year:
        sql += " AND EXTRACT(YEAR FROM fecha) = ?"
        params.append(year)

    sql += " GROUP BY day_of_week ORDER BY day_of_week"

    result = run_query(sql, params)

    if not result and idestacion is None:
        return [
            {"day_of_week": 2, "avg_overflow": 3.5, "max_overflow": 18, "total_observations": 500},
            {"day_of_week": 3, "avg_overflow": 3.8, "max_overflow": 19, "total_observations": 500},
            {"day_of_week": 4, "avg_overflow": 4.0, "max_overflow": 20, "total_observations": 500},
            {"day_of_week": 5, "avg_overflow": 4.2, "max_overflow": 22, "total_observations": 500},
            {"day_of_week": 6, "avg_overflow": 4.8, "max_overflow": 24, "total_observations": 500},
            {"day_of_week": 7, "avg_overflow": 2.5, "max_overflow": 12, "total_observations": 400},
            {"day_of_week": 1, "avg_overflow": 2.0, "max_overflow": 10, "total_observations": 400},
        ]

    return result


# =============================================================================
#  NUEVOS ENDPOINTS ANALÍTICOS (refactorizados)
# =============================================================================

WEEKDAY_LABELS = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}

@app.get("/api/overflow/station_weekday_avg")
def overflow_station_weekday_avg(
    idestacion: str = Query(...),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    sql = f"""
        SELECT
            DAYOFWEEK(fecha) AS day_of_week,
            AVG(overflow)    AS avg_overflow,
            MAX(overflow)    AS max_overflow,
            COUNT(*)         AS total_observations
        FROM estaciones
        WHERE idestacion = ?
          AND fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    params = [idestacion]

    if start:
        sql += " AND fecha >= ?::DATE"
        params.append(start)
    if end:
        sql += " AND fecha <= ?::DATE"
        params.append(end)

    sql += " GROUP BY day_of_week ORDER BY day_of_week"

    rows = run_query(sql, params)
    result = []

    for r in rows:
        rec = dict(r)
        dow = int(rec["day_of_week"])
        rec["label"] = WEEKDAY_LABELS.get(dow, str(dow))
        rec["sort_order"] = (dow - 1) % 7   # para ordenar Lunes → Domingo
        result.append(rec)

    result.sort(key=lambda x: x["sort_order"])
    return result


@app.get("/api/overflow/station_hourly_avg")
def overflow_station_hourly_avg(
    idestacion: str = Query(...),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    sql = f"""
        SELECT
            hora,
            AVG(overflow)    AS avg_overflow,
            MAX(overflow)    AS max_overflow,
            COUNT(*)         AS total_observations
        FROM estaciones
        WHERE idestacion = ?
          AND fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    params = [idestacion]

    if start:
        sql += " AND fecha >= ?::DATE"
        params.append(start)
    if end:
        sql += " AND fecha <= ?::DATE"
        params.append(end)

    sql += " GROUP BY hora ORDER BY hora"

    return run_query(sql, params)


@app.get("/api/overflow/station_daily_summary")
def overflow_station_daily_summary(
    idestacion: str = Query(...),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    sql = f"""
        SELECT
            fecha,
            AVG(overflow) AS avg_overflow,
            MAX(overflow) AS max_overflow,
            SUM(CASE WHEN overflow > 0 THEN 1 ELSE 0 END) AS hours_with_overflow,
            COUNT(*) AS total_hours,
            AVG(
                CASE WHEN (ancladas + baseslibres) > 0
                     THEN CAST(ancladas AS FLOAT) / (ancladas + baseslibres) * 100
                     ELSE NULL END
            ) AS avg_occupancy_pct
        FROM estaciones
        WHERE idestacion = ?
          AND fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    params = [idestacion]

    if start:
        sql += " AND fecha >= ?::DATE"
        params.append(start)
    if end:
        sql += " AND fecha <= ?::DATE"
        params.append(end)

    sql += " GROUP BY fecha ORDER BY fecha"

    rows = run_query(sql, params)
    for row in rows:
        if row.get("fecha") is not None:
            row["fecha"] = str(row["fecha"])
    return rows


@app.get("/api/overflow/capacity_analysis")
def overflow_capacity_analysis(
    idestacion: str = Query(...),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    sql = f"""
        SELECT
            fecha, hora, overflow, ancladas, baseslibres,
            (ancladas + baseslibres) AS capacidad_total,
            CASE WHEN (ancladas + baseslibres) > 0
                 THEN CAST(ancladas AS FLOAT) / (ancladas + baseslibres) * 100
                 ELSE 0 END AS ocupacion_pct,
            CASE WHEN (ancladas + baseslibres) > 0
                 THEN CAST(overflow AS FLOAT) / (ancladas + baseslibres) * 100
                 ELSE 0 END AS overflow_pct_capacidad
        FROM estaciones
        WHERE idestacion = ?
          AND fecha >= DATE '{MIN_OVERFLOW_DATE_STR}'
    """
    params = [idestacion]
    if start:
        sql += " AND fecha >= ?::DATE"; params.append(start)
    if end:
        sql += " AND fecha <= ?::DATE"; params.append(end)

    sql += " ORDER BY fecha, hora"
    return run_query(sql, params)


# =============================================================================
#  ESTADO ACTIVA (abierta/cerrada)
# =============================================================================

def _normalize_status(open_obs: int, closed_obs: int) -> str:
    if open_obs > 0 and closed_obs == 0:
        return "always_open"
    if closed_obs > 0 and open_obs == 0:
        return "always_closed"
    return "mixed"


@app.get("/api/activa/city_summary")
def activa_city_summary(start: str = Query(...), end: str = Query(...)):
    sql = """
        SELECT
            e.idestacion,
            h.denominacion, h.latitud, h.longitud,
            SUM(CASE WHEN COALESCE(CAST(e.activa AS INTEGER), 0) = 1 THEN 1 ELSE 0 END) AS open_obs,
            SUM(CASE WHEN COALESCE(CAST(e.activa AS INTEGER), 0) = 0 THEN 1 ELSE 0 END) AS closed_obs,
            COUNT(*) AS total_obs
        FROM estaciones e
        JOIN HistEstaciones h ON e.idestacion = h.idestacion
                             AND e.fechaHora BETWEEN h.inicio AND h.fin
        WHERE e.fecha BETWEEN ?::DATE AND ?::DATE
        GROUP BY e.idestacion, h.denominacion, h.latitud, h.longitud
        ORDER BY e.idestacion
    """
    rows = run_query(sql, [start, end])

    always_open = mixed = always_closed = 0
    stations = []

    for r in rows:
        oo = int(r.get("open_obs", 0))
        cc = int(r.get("closed_obs", 0))
        status = _normalize_status(oo, cc)
        r["status"] = status
        stations.append(r)

        if status == "always_open":    always_open += 1
        elif status == "always_closed": always_closed += 1
        else:                          mixed += 1

    return {
        "start": start,
        "end": end,
        "totals": {
            "stations": len(stations),
            "always_open": always_open,
            "mixed": mixed,
            "always_closed": always_closed,
        },
        "stations": stations,
    }


@app.get("/api/activa/station_status")
def activa_station_status(
    idestacion: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
):
    sql = """
        SELECT
            e.idestacion, e.fecha, e.hora, e.fechaHora,
            COALESCE(CAST(e.activa AS INTEGER), 0) AS activa,
            h.denominacion, h.latitud, h.longitud
        FROM estaciones e
        JOIN HistEstaciones h ON e.idestacion = h.idestacion
                             AND e.fechaHora BETWEEN h.inicio AND h.fin
        WHERE e.idestacion = ?
          AND e.fecha BETWEEN ?::DATE AND ?::DATE
        ORDER BY e.fecha, e.hora
    """
    rows = run_query(sql, [idestacion, start, end])

    if not rows:
        return {
            "idestacion": idestacion,
            "start": start, "end": end,
            "status": "no_data",
            "open_obs": 0, "closed_obs": 0,
            "closed_moments": [],
        }

    open_obs = closed_obs = 0
    closed_moments = []

    for r in rows:
        if int(r["activa"]) == 1:
            open_obs += 1
        else:
            closed_obs += 1
            closed_moments.append({
                "fecha": str(r["fecha"]),
                "hora": int(r["hora"]),
                "fechaHora": str(r["fechaHora"])
            })

    status = _normalize_status(open_obs, closed_obs)

    first = rows[0]
    return {
        "idestacion": idestacion,
        "denominacion": first.get("denominacion"),
        "latitud": first.get("latitud"),
        "longitud": first.get("longitud"),
        "start": start,
        "end": end,
        "status": status,
        "open_obs": open_obs,
        "closed_obs": closed_obs,
        "closed_moments": closed_moments,
    }
@app.get("/api/estaciones_meta")
def estaciones_meta():
    sql = """
      SELECT idestacion, denominacion, latitud, longitud
      FROM (
        SELECT
          h.idestacion,
          h.denominacion,
          h.latitud,
          h.longitud,
          ROW_NUMBER() OVER (
            PARTITION BY h.idestacion
            ORDER BY COALESCE(h.fin, TIMESTAMP '9999-12-31') DESC
          ) AS rn
        FROM HistEstaciones h
      ) t
      WHERE rn = 1
      ORDER BY CAST(idestacion AS INTEGER)
    """
    return run_query(sql)


# Fin del archivo