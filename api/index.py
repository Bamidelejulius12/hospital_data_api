from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, text
import pandas as pd
import io
import os
from dotenv import load_dotenv
load_dotenv(override=True)

app = FastAPI(title="Hospital Capacity API")

# Your Render PostgreSQL URL
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

@app.get("/")
def root():
    return {
        "api": "Hospital Capacity API",
        "status": "connected",
        "endpoints": {
            "tables": "/tables",
            "query": "/query?sql=SELECT * FROM admissions LIMIT 10",
            "download": "/download/admissions?format=csv"
        }
    }

@app.get("/tables")
def list_tables():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
        tables = [row[0] for row in result]
        return {"tables": tables}

@app.get("/query")
def execute_query(
    sql: str = Query(..., description="SQL SELECT query"),
    limit: int = Query(1000000, description="Max rows"),
    format: str = Query("json", description="json or csv")
):
    sql_upper = sql.upper().strip()
    
    # Block write operations
    forbidden = ['DELETE', 'DROP', 'ALTER', 'TRUNCATE']
    for keyword in forbidden:
        if keyword in sql_upper:
            raise HTTPException(400, "Write operations not allowed")
    
    if not sql_upper.startswith('SELECT'):
        raise HTTPException(400, "Only SELECT queries allowed")
    
    try:
        # Add LIMIT if not present
        if 'LIMIT' not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {limit}"
        
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
            
            if format == "csv":
                csv_data = df.to_csv(index=False)
                return StreamingResponse(
                    io.StringIO(csv_data),
                    media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=data.csv"}
                )
            else:
                return {
                    "row_count": len(df),
                    "columns": list(df.columns),
                    "data": df.to_dict(orient='records')
                }
    except Exception as e:
        raise HTTPException(400, str(e))

@app.get("/download/{table_name}")
def download_table(table_name: str, format: str = "csv"):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            
            if format == "csv":
                csv_data = df.to_csv(index=False)
                return StreamingResponse(
                    io.StringIO(csv_data),
                    media_type="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={table_name}.csv"}
                )
            else:
                return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(400, str(e))

import uvicorn.workers

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
