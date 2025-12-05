import csv
import re
import sqlite3
import sys
import click
from . import database

def query_db(db_path, sdate=None, edate=None, date=None, targets=None, pattern=None, output=None):
    """Queries the database and prints the results."""
    conn = database.get_db_connection(db_path)
    conn.create_function("REGEXP", 2, regexp)
    cursor = conn.cursor()

    query = "SELECT DISTINCT s.StudyDescription, s.SeriesDescription, s.PatientName, s.PatientID, s.StudyDate, s.SeriesInstanceUID FROM series s"
    conditions = []
    params = []

    if date:
        conditions.append("s.StudyDate = ?")
        params.append(date)
    if sdate:
        conditions.append("s.StudyDate >= ?")
        params.append(sdate)
    if edate:
        conditions.append("s.StudyDate <= ?")
        params.append(edate)

    if pattern and targets:
        target_conditions = []
        for target in targets:
            target_conditions.append(f"s.{target} REGEXP ?")
            params.append(pattern)
        conditions.append("(" + " OR ".join(target_conditions) + ")")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cursor.execute(query, params)
    results = cursor.fetchall()
    
    if output:
        writer = csv.writer(open(output, 'w', newline=''))
    else:
        writer = csv.writer(sys.stdout)
        
    writer.writerow(['StudyDescription', 'SeriesDescription', 'PatientName', 'PatientID', 'StudyDate', 'SeriesInstanceUID'])
    writer.writerows(results)

    click.echo(f"Found {len(results)} matching series.", err=True)

    conn.close()

def regexp(expr, item):
    """Regex function for SQLite."""
    reg = re.compile(expr)
    return reg.search(item) is not None
