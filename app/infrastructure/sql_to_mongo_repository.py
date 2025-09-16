import pandas as pd
from sqlalchemy import create_engine, inspect, text
from pymongo import MongoClient


class SQLToMongoRepository:
    def __init__(self, sql_conn_str: str, mongo_conn_str: str, mongo_db_name: str):
        self.sql_conn_str = sql_conn_str
        self.mongo_conn_str = mongo_conn_str
        self.mongo_db_name = mongo_db_name

    def migrate_table(self, table_name: str) -> dict:
        try:
            # Connect to SQL Server
            engine = create_engine(self.sql_conn_str)
            conn = engine.connect()

            # Connect to MongoDB
            mongo_client = MongoClient(self.mongo_conn_str)
            mongo_db = mongo_client[self.mongo_db_name]

            # Check if table exists
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            if table_name not in tables:
                return {"success": False, "message": f"Table '{table_name}' does not exist in SQL Server."}

            # Read data from SQL Server
            df = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)

            # Handle datetime columns with NaT values
            for col in df.select_dtypes(include=["datetimetz", "datetime64[ns]"]).columns:
                df[col] = df[col].astype(str).replace("NaT", None)

            # Convert dataframe to dict
            data = df.to_dict(orient="records")

            if data:
                mongo_db[table_name].insert_many(data)
                return {"success": True, "message": f"Table '{table_name}' migrated successfully to MongoDB!"}
            else:
                return {"success": False, "message": f"Table '{table_name}' is empty, nothing to migrate."}

        except Exception as e:
            return {"success": False, "message": str(e)}

        finally:
            if "conn" in locals():
                conn.close()
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from pymongo import MongoClient


class SQLToMongoRepository:
    def __init__(self, sql_conn_str: str, mongo_conn_str: str, mongo_db_name: str):
        self.sql_conn_str = sql_conn_str
        self.mongo_conn_str = mongo_conn_str
        self.mongo_db_name = mongo_db_name

    def migrate_table(self, table_name: str) -> dict:
        try:
            # Connect to SQL Server
            engine = create_engine(self.sql_conn_str)
            conn = engine.connect()

            # Connect to MongoDB
            mongo_client = MongoClient(self.mongo_conn_str)
            mongo_db = mongo_client[self.mongo_db_name]

            # Check if table exists
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            if table_name not in tables:
                return {"success": False, "message": f"Table '{table_name}' does not exist in SQL Server."}

            # Read data from SQL Server
            df = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)

            # Handle datetime columns with NaT values
            for col in df.select_dtypes(include=["datetimetz", "datetime64[ns]"]).columns:
                df[col] = df[col].astype(str).replace("NaT", None)

            # Convert dataframe to dict
            data = df.to_dict(orient="records")

            if data:
                mongo_db[table_name].insert_many(data)
                return {"success": True, "message": f"Table '{table_name}' migrated successfully to MongoDB!"}
            else:
                return {"success": False, "message": f"Table '{table_name}' is empty, nothing to migrate."}

        except Exception as e:
            return {"success": False, "message": str(e)}

        finally:
            if "conn" in locals():
                conn.close()
