import os
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import csv
import sys
import re
import shutil
from pathlib import Path

import click
from pydicom import dcmread
from pynetdicom import AE, evt, AllStoragePresentationContexts, ALL_TRANSFER_SYNTAXES
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove

from . import project, database

def linux_safe_name(name):
    """Return a linux-safe version of a string."""
    name = re.sub(r'[\s/\\?%*:|"<>]', '_', name)
    return name

def get_subject_name(ds):
    """Return a subject name from a DICOM dataset."""
    if 'PatientName' in ds and ds.PatientName:
        # pydicom returns a PersonName object, which can be converted to a string
        patient_name = str(ds.PatientName)
        # DICOM standard for person names is LastName^FirstName^MiddleName
        parts = patient_name.split('^')
        if len(parts) >= 2:
            last_name, first_name = parts[0], parts[1]
            middle_name = parts[2] if len(parts) > 2 else ''
            
            # Use initial for middle name if it exists
            middle_initial = f"_{middle_name[0]}" if middle_name else ''

            return f"{first_name}{middle_initial}_{last_name}"

    if 'PatientID' in ds and ds.PatientID:
        return f"Patient_{ds.PatientID}"
    
    return f"Study_{ds.StudyInstanceUID}"


def handle_store(event, output_dir):
    """Handle a C-STORE request event."""
    try:
        ds = event.dataset
        ds.file_meta = event.file_meta

        subject_name = get_subject_name(ds)
        series_description = ds.get("SeriesDescription", "UNKNOWN_SERIES")
        
        session_name = linux_safe_name(series_description)
        subject_dir_name = linux_safe_name(subject_name)

        series_dir = os.path.join(output_dir, subject_dir_name, session_name)
        if not os.path.exists(series_dir):
            os.makedirs(series_dir)

        file_path = os.path.join(series_dir, f"{ds.SOPInstanceUID}.dcm")
        ds.save_as(file_path, write_like_original=False)
        
        return 0x0000  # Success
    except Exception as e:
        click.echo(f"Error handling C-STORE: {e}", err=True)
        return 0xA700 # Out of resources

def run_scp(ae, output_dir, scp_port, stop_event):
    """Run the C-STORE SCP."""
    handlers = [(evt.EVT_C_STORE, handle_store, [output_dir])]
    
    ae.supported_contexts = AllStoragePresentationContexts
    
    # Start the server and listen for the stop event
    with ae.start_server(("", scp_port), block=False, evt_handlers=handlers) as scp:
        stop_event.wait() # Block until the stop event is set
        scp.shutdown()


def download_series(series_info, pacs_config, my_aet, scp_port):
    """Send a C-MOVE request for a single series."""

    def on_abort(event):
        click.echo(f"Association Aborted: {event.source} -> {event.reason}", err=True)

    handlers = [(evt.EVT_ABORTED, on_abort)]

    ae = AE()
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
    
    click.echo(f"Requesting association with {pacs_config['host']}:{pacs_config['port']} (AET: {pacs_config['aetitle']})", err=True)
    assoc = ae.associate(pacs_config['host'], int(pacs_config['port']), ae_title=pacs_config['aetitle'], evt_handlers=handlers, debug_logger=click.echo)
    
    if assoc.is_established:
        click.echo("Association established.", err=True)
        ds = dcmread()
        ds.QueryRetrieveLevel = 'SERIES'
        ds.StudyInstanceUID = series_info['StudyInstanceUID']
        ds.SeriesInstanceUID = series_info['SeriesInstanceUID']

        click.echo(f"Sending C-MOVE request for SeriesInstanceUID: {ds.SeriesInstanceUID}", err=True)
        responses = assoc.send_c_move(ds, my_aet, StudyRootQueryRetrieveInformationModelMove)
        
        for (status, identifier) in responses:
            if status:
                click.echo(f"C-MOVE response status: {status.Status:04x}", err=True)
                if status.Status not in (0xFF00, 0x0000): # Pending or Success
                    click.echo(f"C-MOVE failed for {series_info['SeriesInstanceUID']} with status: {status.Status:04x}", err=True)
            else:
                click.echo("No status returned for C-MOVE response.", err=True)

        assoc.release()
        click.echo("Association released.", err=True)
    else:
        click.echo("Failed to associate with PACS for C-MOVE. Check AE titles, host, and port.", err=True)


def download_project(project_name, threads, output, my_aet, scp_port, zip_project, input_file=None):
    """Download all series for a project."""
    proj_config = project.get_project_config(project_name)
    if not proj_config:
        click.echo(f"Project '{project_name}' not found.", err=True)
        return

    db_path = proj_config['database_path']
    if not os.path.exists(db_path):
        click.echo(f"Database for project '{project_name}' not found.", err=True)
        return

    # Create output directory
    date_str = datetime.now().strftime('%Y%m%d')
    safe_project_name = linux_safe_name(project_name)
    output_dir = os.path.join(output, f"{safe_project_name}_{date_str}")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Start SCP in a background thread
    scp_ae = AE(ae_title=my_aet)
    stop_event = threading.Event()
    scp_thread = threading.Thread(target=run_scp, args=(scp_ae, output_dir, scp_port, stop_event))
    scp_thread.start()
    click.echo(f"SCP server started on port {scp_port} with AE title {my_aet}", err=True)

    # Give the SCP a moment to start up
    time.sleep(1)

    # Get series to download
    conn = database.get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT StudyInstanceUID, SeriesInstanceUID FROM series')
    all_series = [{'StudyInstanceUID': r[0], 'SeriesInstanceUID': r[1]} for r in cursor.fetchall()]
    conn.close()

    series_to_download = all_series
    if input_file:
        uids_to_download = set()
        if input_file == '-':
            reader = csv.reader(sys.stdin)
        else:
            f = open(input_file, 'r')
            reader = csv.reader(f)
            
        header = next(reader)
        uid_col = header.index('SeriesInstanceUID')
        for row in reader:
            uids_to_download.add(row[uid_col])
        
        if input_file != '-':
            f.close()

        series_to_download = [s for s in all_series if s['SeriesInstanceUID'] in uids_to_download]
        click.echo(f"Found {len(series_to_download)} series to download from input file.", err=True)


    pacs_config = proj_config['pacs']

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(download_series, series, pacs_config, my_aet, scp_port): series for series in series_to_download}
        for future in futures:
            future.result() # wait for all downloads to be initiated

    click.echo("All download requests sent. Waiting for SCP to receive files...", err=True)
    
    # Give a moment for the last few files to arrive
    time.sleep(5) 
    
    # Gracefully shut down the SCP
    stop_event.set()
    scp_thread.join()
    
    if zip_project:
        click.echo("Zipping subject directories...", err=True)
        output_path = Path(output_dir)
        for subject_dir in output_path.iterdir():
            if subject_dir.is_dir():
                shutil.make_archive(str(subject_dir), 'zip', subject_dir)
                shutil.rmtree(subject_dir)
        click.echo("Zipping complete.", err=True)

    click.echo("Download process finished.", err=True)
