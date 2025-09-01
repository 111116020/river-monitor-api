import datetime
import io
import json
import struct
from typing import Any
import mysql.connector

class ModelDatabase:
    _db_table_name = "WaterLevel"

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
    
    def connect(self):
        self.conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
    
    def insert_data(
            self,
            timestamp: str,
            river_name: str,
            est_level: float,
            points: list[tuple[float, float]],
            country_name="",
            basin_name="",
        ):
        
        if not self.conn.is_connected():
            self.conn.reconnect()

        model_points = io.BytesIO()
        for x, y in points:
            model_points.write(struct.pack("f", x))
            model_points.write(struct.pack("f", y))

        sql = f"""INSERT INTO WaterLevel
                (upload_time, river_name, est_level, model_points, country_name, basin_name) 
                VALUES (%s, %s, %s, %s, %s, %s)"""
        
        with self.conn.cursor() as cursor:
            cursor.execute(sql, (
                timestamp, 
                river_name, 
                est_level, 
                model_points.getvalue(),
                country_name,
                basin_name
            ))
            self.conn.commit()
    
    def retrieve_data(self, start_ts: int | None = None, end_ts: int | None = None):
        if not self.conn.is_connected():
            self.conn.reconnect()

        sql_query = "SELECT UNIX_TIMESTAMP(upload_time), river_name, est_level, model_points, country_name, basin_name FROM WaterLevel "
        if start_ts and end_ts:
            sql_query += f"""WHERE UNIX_TIMESTAMP(upload_time) BETWEEN {start_ts} AND {end_ts}"""
        elif start_ts:
            sql_query += f"""WHERE UNIX_TIMESTAMP(upload_time) >= {start_ts}"""
        elif end_ts:
            sql_query += f"""WHERE UNIX_TIMESTAMP(upload_time) <= {end_ts}"""
        else:
            sql_query += "ORDER BY upload_time DESC LIMIT 1"

        with self.conn.cursor() as cursor:
            cursor.execute(sql_query, map_results=True)
            for upload_time, river_name, est_level, model_points, country_name, basin_name in cursor.fetchall():
                yield {
                    "timestamp": upload_time,
                    "river_name": river_name,
                    "est_level": float(est_level),
                    "points": [
                        (struct.unpack("f", model_points[i:i+4])[0], struct.unpack("f", model_points[i+4:i+8])[0])
                        for i in range(0, len(model_points), 8)
                    ],
                    "country_name": country_name,
                    "basin_name": basin_name
                }
    
    def close(self):
        self.conn.close()
