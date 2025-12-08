import os
import pydicom
import click
from . import database
from pydicom.errors import InvalidDicomError
from concurrent.futures import ThreadPoolExecutor
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TaskID
import time

def get_subdirectories(path):
    """Returns a list of all subdirectories in the given path."""
    subdirectories = []
    for dirpath, dirnames, filenames in os.walk(path):
        if filenames:  # Only include directories that contain files
            subdirectories.append(dirpath)
    return subdirectories

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

def process_subdirectory(subdirectory_path, archive_path, db_path, progress, task_id):
    """Processes all DICOM files in a single subdirectory."""
    files = [os.path.join(subdirectory_path, f) for f in os.listdir(subdirectory_path) if os.path.isfile(os.path.join(subdirectory_path, f))]
    progress.update(task_id, total=len(files), description=f"[cyan]Processing: {os.path.basename(subdirectory_path)}[/cyan]")
    
    for file_path in files:
        error = process_file(file_path, archive_path, db_path)
        if error:
            progress.console.print(error)
        progress.update(task_id, advance=1)
    
    progress.update(task_id, description=f"[green]Finished: {os.path.basename(subdirectory_path)}[/green]")


def index_archive(archive_path, db_path, append=False, threads=4):
    """Indexes the DICOM files in the archive path and stores the metadata in the database."""
    if not os.path.exists(archive_path):
        click.echo(f"Error: Archive path not found at {archive_path}")
        return

    conn = database.get_db_connection(db_path)
    if not append:
        database.create_tables(conn)
    conn.close()

    click.echo("Finding subdirectories to index...")
    subdirectories = get_subdirectories(archive_path)
    
    if not subdirectories:
        click.echo("No subdirectories with files found to index.")
        return

    click.echo(f"{len(subdirectories)} subdirectories found.")
    if not click.confirm("Do you want to proceed with indexing?"):
        click.echo("Indexing cancelled.")
        return

    with Progress(
        TextColumn("[bold blue]{task.description}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.0f}%",
        "•",
        TimeRemainingColumn(),
        transient=True,
    ) as progress:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            tasks = {executor.submit(process_subdirectory, subdir, archive_path, db_path, progress, progress.add_task(f"Queued: {os.path.basename(subdir)}", total=1)): subdir for subdir in subdirectories}
            
            for future in tasks:
                future.result() # wait for all tasks to complete

    click.echo(f"Indexing complete. Database updated at {db_path}")