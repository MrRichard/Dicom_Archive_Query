import os
import pydicom
import click
from . import database
from pydicom.errors import InvalidDicomError
from concurrent.futures import ThreadPoolExecutor
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TaskID
import time
from pynetdicom import AE, evt
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind

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

def index_pacs(proj_config):
    """Indexes a project from a PACS based on a list of accession numbers."""
    db_path = proj_config['database_path']
    pacs_config = proj_config['pacs']
    target_list_path = proj_config['target_list']
    start_at_line = proj_config.get('start_at_line')
    start_at_accession = proj_config.get('start_at_accession')

    conn = database.get_db_connection(db_path)
    database.create_tables(conn)
    conn.close()

    with open(target_list_path, 'r') as f:
        accession_numbers = [line.strip() for line in f if line.strip()]
    
    if start_at_line is not None:
        if 1 <= start_at_line <= len(accession_numbers):
            accession_numbers = accession_numbers[start_at_line - 1:]
            click.echo(f"Starting at line {start_at_line}.")
        else:
            click.echo(f"Warning: --start-at-line value {start_at_line} is out of range. Indexing from the beginning.")
    
    if start_at_accession is not None:
        try:
            start_index = accession_numbers.index(start_at_accession)
            accession_numbers = accession_numbers[start_index:]
            click.echo(f"Starting at accession number {start_at_accession}.")
        except ValueError:
            click.echo(f"Warning: Accession number '{start_at_accession}' not found in the target list. Indexing from the beginning.")

    ae = AE()
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

    assoc = ae.associate(pacs_config['host'], int(pacs_config['port']), ae_title=pacs_config['aetitle'])

    if assoc.is_established:
        click.echo("PACS association established.")

        with Progress(
            TextColumn("[bold blue]{task.description}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TimeRemainingColumn(),
            transient=True,
        ) as progress:
            task = progress.add_task("[cyan]Querying PACS...", total=len(accession_numbers))

            for acc_num in accession_numbers:
                progress.update(task, advance=1, description=f"[cyan]Querying acc: {acc_num}")

                # Find StudyInstanceUID for the accession number
                study_uid = None
                ds = pydicom.Dataset()
                ds.QueryRetrieveLevel = 'STUDY'
                ds.AccessionNumber = acc_num
                ds.StudyInstanceUID = ''
                
                responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
                for (status, identifier) in responses:
                    if status.Status in (0xFF00, 0xFF01): # Pending
                        if identifier and 'StudyInstanceUID' in identifier:
                            study_uid = identifier.StudyInstanceUID
                    elif status.Status != 0:
                        click.echo(f"C-FIND failed for {acc_num} with status: {status.Status:04x}")
                
                if study_uid:
                    # Now find series for this study
                    ds = pydicom.Dataset()
                    ds.QueryRetrieveLevel = 'SERIES'
                    ds.StudyInstanceUID = study_uid
                    ds.SeriesInstanceUID = ''
                    ds.SeriesDescription = ''
                    ds.PatientName = ''
                    ds.PatientID = ''
                    ds.StudyDate = ''
                    ds.StudyDescription = ''

                    series_responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
                    conn = database.get_db_connection(db_path)
                    for (status, identifier) in series_responses:
                        if status.Status in (0xFF00, 0xFF01):
                            if identifier and 'SeriesInstanceUID' in identifier:
                                metadata = {
                                    'StudyInstanceUID': study_uid,
                                    'SeriesInstanceUID': identifier.SeriesInstanceUID,
                                    'StudyDescription': identifier.get('StudyDescription'),
                                    'SeriesDescription': identifier.get('SeriesDescription'),
                                    'PatientName': str(identifier.get('PatientName', '')),
                                    'PatientID': identifier.get('PatientID'),
                                    'StudyDate': identifier.get('StudyDate'),
                                    'archive_path': f"pacs://{pacs_config['aetitle']}",
                                }
                                database.insert_series_metadata(conn, metadata)
                            elif identifier:
                                click.echo(f"Warning: Series found for study {study_uid} but it is missing SeriesInstanceUID. Skipping.")
                        elif status.Status != 0:
                            click.echo(f"Series C-FIND failed for study {study_uid} with status: {status.Status:04x}")
                    conn.close()

        assoc.release()
        click.echo("PACS association released.")
    else:
        click.echo("Failed to associate with PACS.")