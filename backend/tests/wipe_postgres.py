import os
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

with engine.connect() as connection:
    inspector = inspect(engine)
    schema = 'sec_app'
    
    # Get all table names in the schema
    tables = inspector.get_table_names(schema=schema)
    
    # Truncate each table
    for table_name in tables:
        print(f"Clearing table: {table_name}")
        connection.execute(text(f'TRUNCATE TABLE {schema}."{table_name}" RESTART IDENTITY CASCADE;'))
    
    connection.commit()
    print(f"\nAll tables in schema '{schema}' have been cleared.")
