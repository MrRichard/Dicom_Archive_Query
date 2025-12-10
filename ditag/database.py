import sqlite3
import os
import threading

db_lock = threading.Lock()

def get_db_connection(db_path):
    """Establishes a connection to the database."""
    return sqlite3.connect(db_path, check_same_thread=False)

def create_tables(conn):
    """Creates the necessary tables in the database."""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            StudyInstanceUID TEXT,
            SeriesInstanceUID TEXT UNIQUE,
            StudyDescription TEXT,
            SeriesDescription TEXT,
            PatientName TEXT,
            PatientID TEXT,
            StudyDate TEXT,
            archive_path TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS instances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER,
            SOPInstanceUID TEXT UNIQUE,
            file_path TEXT,
            FOREIGN KEY (series_id) REFERENCES series (id)
        )
    ''')
    conn.commit()

def insert_dicom_metadata(conn, metadata):
    """Inserts DICOM metadata into the database."""
    with db_lock:
        cursor = conn.cursor()
        
        # Insert or get series
        cursor.execute('''
            SELECT id FROM series WHERE SeriesInstanceUID = ?
        ''', (metadata['SeriesInstanceUID'],))
        result = cursor.fetchone()
        
        if result:
            series_id = result[0]
        else:
            cursor.execute('''
                INSERT INTO series (
                    StudyInstanceUID, SeriesInstanceUID, StudyDescription, 
                    SeriesDescription, PatientName, PatientID, StudyDate, archive_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metadata.get('StudyInstanceUID', ''),
                metadata.get('SeriesInstanceUID', ''),
                metadata.get('StudyDescription', ''),
                metadata.get('SeriesDescription', ''),
                metadata.get('PatientName', ''),
                metadata.get('PatientID', ''),
                metadata.get('StudyDate', ''),
                metadata.get('archive_path', '')
            ))
            series_id = cursor.lastrowid

        # Insert instance
        cursor.execute('''
            INSERT OR IGNORE INTO instances (series_id, SOPInstanceUID, file_path)
            VALUES (?, ?, ?)
        ''', (series_id, metadata['SOPInstanceUID'], metadata['file_path']))
        
        conn.commit()

def insert_series_metadata(conn, metadata):
    """Inserts DICOM series metadata into the database."""
    with db_lock:
        cursor = conn.cursor()
        
        # Insert or get series
        cursor.execute('''
            SELECT id FROM series WHERE SeriesInstanceUID = ?
        ''', (metadata['SeriesInstanceUID'],))
        result = cursor.fetchone()
        
        if not result:
            cursor.execute('''
                INSERT INTO series (
                    StudyInstanceUID, SeriesInstanceUID, StudyDescription, 
                    SeriesDescription, PatientName, PatientID, StudyDate, archive_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metadata.get('StudyInstanceUID', ''),
                metadata.get('SeriesInstanceUID', ''),
                metadata.get('StudyDescription', ''),
                metadata.get('SeriesDescription', ''),
                metadata.get('PatientName', ''),
                metadata.get('PatientID', ''),
                metadata.get('StudyDate', ''),
                metadata.get('archive_path', '')
            ))
        
        conn.commit()
