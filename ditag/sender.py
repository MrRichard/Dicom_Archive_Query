import csv
import sys
import click
import pydicom
from pynetdicom import AE, debug_logger
from pynetdicom.presentation import build_context
from . import database

debug_logger()

def send_dicoms(db_path, myaet, pacs_aet, destination, port, input_file=None):
    """Sends DICOM files to a PACS destination."""
    
    series_uids = []
    if input_file:
        with open(input_file, 'r') as f:
            reader = csv.reader(f)
            # Skip header if it exists
            try:
                first_row = next(reader)
                if 'SeriesInstanceUID' not in first_row:
                    series_uids.append(first_row[5])
            except StopIteration:
                pass # Empty file
            for row in reader:
                if row:
                    series_uids.append(row[5]) # Assumes SeriesInstanceUID is the 6th column
    else:
        reader = csv.reader(sys.stdin)
        try:
            next(reader) # skip header
        except StopIteration:
            pass
        for row in reader:
            if row:
                series_uids.append(row[5])

    if not series_uids:
        click.echo("No series to send.")
        return

    conn = database.get_db_connection(db_path)
    cursor = conn.cursor()

    ae = AE(ae_title=myaet)
    
    # Dynamically build presentation contexts
    sop_classes = set()
    for series_uid in series_uids:
        cursor.execute('''
            SELECT i.file_path FROM instances i
            JOIN series s ON i.series_id = s.id
            WHERE s.SeriesInstanceUID = ?
            LIMIT 1
        ''', (series_uid,))
        file_path = cursor.fetchone()
        if file_path:
            try:
                ds = pydicom.dcmread(file_path[0], stop_before_pixels=True)
                sop_classes.add(ds.SOPClassUID)
            except Exception as e:
                click.echo(f"Could not read SOP Class from {file_path[0]}: {e}")

    ae.requested_contexts = [build_context(sop) for sop in sop_classes]
    if not ae.requested_contexts:
        click.echo("No valid SOP classes found for the series to be sent. Aborting.")
        conn.close()
        return

    assoc = ae.associate(destination, port, ae_title=pacs_aet)

    if assoc.is_established:
        for series_uid in series_uids:
            cursor.execute('''
                SELECT i.file_path FROM instances i
                JOIN series s ON i.series_id = s.id
                WHERE s.SeriesInstanceUID = ?
            ''', (series_uid,))
            file_paths = [row[0] for row in cursor.fetchall()]

            for file_path in file_paths:
                try:
                    ds = pydicom.dcmread(file_path)
                    status = assoc.send_c_store(ds)
                    if status:
                        click.echo(f"C-STORE request status: 0x{status.Status:04x} for {file_path}")
                    else:
                        click.echo(f"Connection timed out, was aborted or received invalid response for {file_path}")
                except Exception as e:
                    click.echo(f"Error sending {file_path}: {e}")
        
        assoc.release()
    else:
        click.echo("Association rejected, aborted or never connected")

    conn.close()
