from pydantic import BaseModel
from fastapi import HTTPException
from datetime import datetime

from fastapi import FastAPI, HTTPException
from typing import List, Dict, Any
import sqlite3
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE = "data_new.db"

def get_connection():
    """Return a connection to the SQLite database."""
    conn = sqlite3.connect(DATABASE)
    return conn

def create_table_from_header(header: Dict[str, Any], table_name: str) -> None:
    sorted_keys = sorted(header.keys(), key=lambda x: int(x))
    columns = []
    for key in sorted_keys:
        col_name = header[key] if header[key] is not None else f"column_{key}"
        columns.append(f'"{col_name}" TEXT')
    columns_sql = ", ".join(columns)
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
    
    conn = get_connection()
    conn.execute(create_sql)
    conn.commit()
    conn.close()

def insert_row(header: Dict[str, Any], row: Dict[str, Any], table_name: str) -> None:
    sorted_keys = sorted(header.keys(), key=lambda x: int(x))
    column_names = []
    placeholders = []
    values = []
    for key in sorted_keys:
        col_name = header[key] if header[key] is not None else f"column_{key}"
        column_names.append(f'"{col_name}"')
        placeholders.append("?")
        values.append(row.get(key))
    
    columns_sql = ", ".join(column_names)
    placeholders_sql = ", ".join(placeholders)
    insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders_sql});"
    
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(insert_sql, values)
    conn.commit()
    conn.close()

def fetch_all_data(table_name: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()
    data = [dict(row) for row in rows]
    conn.close()
    return data


class ConversionRateResponse(BaseModel):
    date: str
    agent: str
    total_calls: int
    total_bookings: int
    conversion_rate: float

from collections import defaultdict

@app.get("/conversion-rate", response_model=list[dict])
async def get_conversion_rate():
    conn = get_connection()
    cursor = conn.cursor()

    # Fetch all distinct dates from daily2025 and validate them
    cursor.execute("SELECT DISTINCT \"Date\" FROM daily2025")
    date_rows = cursor.fetchall()
    valid_dates = []

    for row in date_rows:
        date_str = row[0]
        try:
            parsed_date = datetime.strptime(date_str, "%m/%d/%Y")
            valid_dates.append(parsed_date)
        except ValueError:
            continue

    agents = {
        "Lynna Goodwin": {
            "aircall_names": ["Lynna Goodwin", "Lynne Goodwin"],
            "hubspot_name": "Lynna Goodwin"
        },
        "Kelvin Emmanuel": {
            "aircall_names": ["Kelvin Emmanuel"],
            "hubspot_name": "Kelvin Emmanuel"
        }
    }

    results_by_date = defaultdict(dict)

    for date in valid_dates:
        base_date = datetime(1899, 12, 30)
        delta = date - base_date
        numeric_date = delta.days  # Removed +1 to fix date offset

        hubspot_date = date.strftime("%d-%m-%Y")
        date_key = date.strftime("%m/%d/%Y")

        # Calculate all_calls and all_missed_calls for the date (across all agents)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_calls_all,
                COALESCE(SUM(
                    CASE WHEN COALESCE("Reason for MIssed Call", '') != '' 
                    THEN 1 ELSE 0 
                    END
                ), 0) as missed_calls_all
            FROM aircall 
            WHERE "Call Date"=? 
            AND Direction=?
            """, (str(numeric_date), 'inbound'))

        all_result = cursor.fetchone()
        total_calls_all = all_result[0]
        missed_calls_all = all_result[1]
        all_missed_percent = round((missed_calls_all / total_calls_all * 100), 2) if total_calls_all > 0 else 0.0

        # Add aggregated totals to results
        results_by_date[date_key]["all_calls"] = total_calls_all
        results_by_date[date_key]["all_missed_calls"] = missed_calls_all
        results_by_date[date_key]["all_missed_calls_percentage"] = all_missed_percent

        # Process individual agents
        for agent_name, config in agents.items():
            try:
                # Query Aircall for total calls and missed calls
                name_conditions = " OR ".join(['"User"=?' for _ in config["aircall_names"]])
                aircall_params = [str(numeric_date), 'inbound'] + config["aircall_names"]  # Ensure numeric_date is string if stored as TEXT
                
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total_calls,
                        COALESCE(SUM(
                            CASE WHEN COALESCE("Reason for MIssed Call", '') != '' 
                            THEN 1 ELSE 0 
                            END
                        ), 0) as missed_calls
                    FROM aircall 
                    WHERE "Call Date"=? 
                    AND Direction=?
                    AND ({name_conditions})
                    """, aircall_params)

                aircall_result = cursor.fetchone()
                total_calls = aircall_result[0]
                missed_calls = aircall_result[1]

                missed_calls_percentage = round((missed_calls / total_calls * 100), 2) if total_calls > 0 else 0.0

                # Query HubSpot for total bookings
                cursor.execute("""
                    SELECT COUNT(*) FROM hubspot 
                    WHERE "Date Booked"=? 
                    AND "Client Contact Method"='Phone' 
                    AND "Booked by"=?
                    """, (hubspot_date, config["hubspot_name"]))
                total_bookings = cursor.fetchone()[0]

                conversion_rate = round((total_bookings / total_calls * 100), 2) if total_calls > 0 else 0.0

                date_key = date.strftime("%m/%d/%Y")
                results_by_date[date_key][agent_name] = {
                    "total_calls": total_calls,
                    # "no_of_missed_calls": missed_calls,
                    # "missed_calls_percentage": missed_calls_percentage,
                    "total_bookings": total_bookings,
                    "conversion_rate": conversion_rate
                }

            except sqlite3.Error as e:
                conn.close()
                raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    conn.close()

    response = [{"date": date, **agents_data} for date, agents_data in results_by_date.items()]
    return response


# The rest of the code (POST and other GET endpoints) remains unchanged as per the original

@app.post("/aircall")
async def post_aircall(data: List[Dict[str, Any]]):
    if not data or len(data) < 2:
        raise HTTPException(
            status_code=400,
            detail="Insufficient data provided. Expecting a header row and at least one data row."
        )
    
    header = data[0]
    create_table_from_header(header, "aircall")
    
    for row in data[1:]:
        insert_row(header, row, "aircall")
    
    return {"message": "Data inserted successfully into aircall."}

@app.post("/hubspot")
async def post_hubspot(data: List[Dict[str, Any]]):
    if not data or len(data) < 2:
        raise HTTPException(
            status_code=400,
            detail="Insufficient data provided. Expecting a header row and at least one data row."
        )
    
    header = data[0]
    create_table_from_header(header, "hubspot")
    
    for row in data[1:]:
        insert_row(header, row, "hubspot")
    
    return {"message": "Data inserted successfully into hubspot."}

@app.post("/daily2025")
async def post_daily2025(data: List[Dict[str, Any]]):
    if not data or len(data) < 2:
        raise HTTPException(
            status_code=400,
            detail="Insufficient data provided. Expecting a header row and at least one data row."
        )
    
    header = data[0]
    create_table_from_header(header, "daily2025")
    
    for row in data[1:]:
        insert_row(header, row, "daily2025")
    
    return {"message": "Data inserted successfully into daily2025."}

@app.get("/daily2025")
async def read_all_data():
    try:
        data = fetch_all_data("daily2025")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"data": data}

@app.get("/{month}")
async def get_month_data(month: str):
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        query = f'SELECT * FROM daily2025 WHERE lower("Date") LIKE ?'
        cur.execute(query, (f"{month.lower()}%",))
        rows = cur.fetchall()
        data = [dict(row) for row in rows]
        conn.close()
        if not data:
            raise HTTPException(status_code=404, detail=f"No data found for month: {month}")
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/{month}/{day}/{year}")
async def get_month_data(month: str, day: str, year: str):
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        query = f'SELECT * FROM daily2025 WHERE lower("Date") = ?'
        cur.execute(query, (f"{month.lower()}/{day.lower()}/{year}",))
        rows = cur.fetchall()
        data = [dict(row) for row in rows]
        conn.close()
        if not data:
            raise HTTPException(status_code=404, detail=f"No data found for date: {month}/{day}")
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/month/{month}")
async def get_months_data(month: str):
    try:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        query = f'SELECT * FROM daily2025 WHERE lower("Date") like ?'
        cur.execute(query, (f"{month}/%",))
        rows = cur.fetchall()
        data = [dict(row) for row in rows]
        conn.close()
        if not data:
            raise HTTPException(status_code=404, detail=f"No data found for date: {month}")
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)