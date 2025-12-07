import os
import pydicom
import click
from . import database
from pydicom.errors import InvalidDicomError
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def process_file(file_path, archive_path, db_path):
    """Processes a single DICOM file and inserts its metadata into the database."""
    try:
        conn = database.get_db_connection(db_path)
        ds = pydicom.dcmread(file_path, stop_before_pixels=True)
        
        if 'SeriesInstanceUID' not in ds or 'SOPInstanceUID' not in ds:
            return f"Skipping {file_path} due to missing required UIDs."

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
        conn.close()
        return None

    except InvalidDicomError:
        return f"Skipping non-DICOM file: {file_path}"
    except Exception as e:
        return f"Could not read {file_path}: {e}"

def index_archive(archive_path, db_path, append=False):
    """Indexes the DICOM files in the archive path and stores the metadata in the database."""
    if not os.path.exists(archive_path):
        click.echo(f"Error: Archive path not found at {archive_path}")
        return

    conn = database.get_db_connection(db_path)
    if not append:
        database.create_tables(conn)
    conn.close()

    click.echo("Finding files to index...")
    filepaths = [os.path.join(root, file) for root, _, files in os.walk(archive_path) for file in files]
    
    with ThreadPoolExecutor() as executor:
        with tqdm(total=len(filepaths), desc="Indexing files") as pbar:
            futures = [executor.submit(process_file, fp, archive_path, db_path) for fp in filepaths]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    tqdm.write(result, file=click.get_text_stream('stderr'))
                pbar.update(1)

    click.echo(f"Indexing complete. Database updated at {db_path}")
