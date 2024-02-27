import pandas as pd
from sqlalchemy import create_engine
import sqlite3

def create_app_data() :
    cursor = sqlite3.connect('eviesens.db')
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS app_data(
    app_data_id INTEGER,
    app_data_name TEXT,
    app_data_value TEXT,
    PRIMARY KEY(app_data_id)
    );
    """)

    cursor.execute("""
        INSERT INTO app_data (app_data_name, app_data_value) VALUES 
        ('WINDOW_COLOR', '#dff9fb'),
        ('FRAME_COLOR', '#badc58')
    """)
    cursor.commit()

def restore_default_app_data() :
    cursor = sqlite3.connect('eviesens.db')
    cursor.execute("""
        UPDATE app_data 
        SET app_data_value = '#dff9fb'
        WHERE app_data_name='WINDOW_COLOR'
    """)

    cursor.execute("""
        UPDATE app_data 
        SET app_data_value = '#badc58'
        WHERE app_data_name='FRAME_COLOR'
    """)
    cursor.commit()

def update_window_color(window_color) :
    cursor = sqlite3.connect('eviesens.db')
    cursor.execute(f"""
        UPDATE app_data 
        SET app_data_value = '{window_color}'
        WHERE app_data_name='WINDOW_COLOR'
    """)
    cursor.commit()

def update_frame_color(frame_color) :
    cursor = sqlite3.connect('eviesens.db')
    cursor.execute(f"""
        UPDATE app_data 
        SET app_data_value = '{frame_color}'
        WHERE app_data_name='FRAME_COLOR'
    """)
    cursor.commit()

def get_window_color() :
    cursor = sqlite3.connect('eviesens.db')
    rs = cursor.execute("SELECT app_data_value FROM app_data WHERE app_data_name='WINDOW_COLOR'")
    for row in rs:
        return row[0]

def get_frame_color() :
    cursor = sqlite3.connect('eviesens.db')
    rs = cursor.execute("SELECT app_data_value FROM app_data WHERE app_data_name='FRAME_COLOR'")
    for row in rs:
        return row[0]
