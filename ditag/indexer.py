import os
import pydicom
import click
from . import database
from pydicom.errors import InvalidDicomError

def index_archive(archive_path, db_path, append=False):
    """Indexes the DICOM files in the archive path and stores the metadata in the database."""
    if not os.path.exists(archive_path):
        click.echo(f"Error: Archive path not found at {archive_path}")
        return

    conn = database.get_db_connection(db_path)
    if not append:
        database.create_tables(conn)

    click.echo("Starting to index files...")
    for root, _, files in os.walk(archive_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                ds = pydicom.dcmread(file_path, stop_before_pixels=True)
                
                if 'SeriesInstanceUID' not in ds or 'SOPInstanceUID' not in ds:
                    click.echo(f"Skipping {file_path} due to missing required UIDs.", err=True)
                    continue

                metadata = {
                    'StudyInstanceUID': ds.get('StudyInstanceUID'),
                    'SeriesInstanceUID': ds.get('SeriesInstanceUID'),
                    'SOPInstanceUID': ds.get('SOPInstanceUID'),
                    'StudyDescription': ds.get('StudyDescription'),
                    'SeriesDescription': ds.get('SeriesDescription'),
                    'PatientName': str(ds.get('PatientName', '')),
                    'PatientID': ds.get('PatientID'),
                    'StudyDate': ds.get('StudyDate'),
                    'archive_path': archive_path,
                    'file_path': file_path
                }
                database.insert_dicom_metadata(conn, metadata)
                click.echo(f"Indexed: {file_path}", err=True)

            except InvalidDicomError:
                click.echo(f"Skipping non-DICOM file: {file_path}", err=True)
            except Exception as e:
                click.echo(f"Could not read {file_path}: {e}", err=True)

    conn.close()
    click.echo(f"Indexing complete. Database updated at {db_path}")
