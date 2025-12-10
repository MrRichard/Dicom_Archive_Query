import os
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import csv
import sys

import click
from pydicom import dcmread
from pynetdicom import AE, evt, AllStoragePresentationContexts, ALL_TRANSFER_SYNTAXES
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove

from . import project, database

def handle_store(event, output_dir):
    """Handle a C-STORE request event."""
    try:
        ds = event.dataset
        ds.file_meta = event.file_meta

        # Create a directory structure
        patient_id = ds.get("PatientID", "UNKNOWN_PATIENT")
        study_date = ds.get("StudyDate", "UNKNOWN_DATE")
        series_num = ds.get("SeriesNumber", "UNKNOWN_SERIES")
        
        # Sanitize directory names
        patient_id = "".join(c for c in patient_id if c.isalnum() or c in ('-', '_')).rstrip()
        study_date = "".join(c for c in study_date if c.isalnum() or c in ('-', '_')).rstrip()
        series_num = "".join(c for c in series_num if c.isalnum() or c in ('-', '_')).rstrip()

        series_dir = os.path.join(output_dir, patient_id, study_date, f"series_{series_num}")
        if not os.path.exists(series_dir):
            os.makedirs(series_dir)

        file_path = os.path.join(series_dir, f"{ds.SOPInstanceUID}.dcm")
        ds.save_as(file_path, write_like_original=False)
        
        return 0x0000  # Success
    except Exception as e:
        click.echo(f"Error handling C-STORE: {e}")
        return 0xA700 # Out of resources

def run_scp(ae, output_dir, scp_port):
    """Run the C-STORE SCP."""
    handlers = [(evt.EVT_C_STORE, handle_store, [output_dir])]
    
    ae.supported_contexts = AllStoragePresentationContexts
    ae.start_server(("", scp_port), block=True, evt_handlers=handlers)

def download_series(series_info, pacs_config, my_aet, scp_port):
    """Send a C-MOVE request for a single series."""
    ae = AE()
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
    
    assoc = ae.associate(pacs_config['host'], int(pacs_config['port']), ae_title=pacs_config['aetitle'])
    
    if assoc.is_established:
        ds = dcmread()
        ds.QueryRetrieveLevel = 'SERIES'
        ds.StudyInstanceUID = series_info['StudyInstanceUID']
        ds.SeriesInstanceUID = series_info['SeriesInstanceUID']

        responses = assoc.send_c_move(ds, my_aet, StudyRootQueryRetrieveInformationModelMove)
        
        for (status, identifier) in responses:
            if status:
                if status.Status not in (0xFF00, 0x0000): # Pending or Success
                    click.echo(f"C-MOVE failed for {series_info['SeriesInstanceUID']} with status: {status.Status:04x}")

        assoc.release()
    else:
        click.echo("Failed to associate with PACS for C-MOVE.")


def download_project(project_name, threads, output, my_aet, scp_port, input_file=None):
    """Download all series for a project."""
    proj_config = project.get_project_config(project_name)
    if not proj_config:
        click.echo(f"Project '{project_name}' not found.")
        return

    db_path = proj_config['database_path']
    if not os.path.exists(db_path):
        click.echo(f"Database for project '{project_name}' not found.")
        return

    # Create output directory
    date_str = datetime.now().strftime('%Y%m%d')
    output_dir = os.path.join(output, f"{project_name}_{date_str}")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Start SCP in a background thread
    scp_ae = AE(ae_title=my_aet)
    scp_thread = threading.Thread(target=run_scp, args=(scp_ae, output_dir, scp_port))
    scp_thread.daemon = True
    scp_thread.start()
    click.echo(f"SCP server started on port {scp_port} with AE title {my_aet}")

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
            reader = csv.reader(open(input_file, 'r'))
        
        header = next(reader)
        uid_col = header.index('SeriesInstanceUID')
        for row in reader:
            uids_to_download.add(row[uid_col])
        
        series_to_download = [s for s in all_series if s['SeriesInstanceUID'] in uids_to_download]
        click.echo(f"Found {len(series_to_download)} series to download from input file.")


    pacs_config = proj_config['pacs']

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(download_series, series, pacs_config, my_aet, scp_port): series for series in series_to_download}
        for future in futures:
            future.result() # wait for all downloads to be initiated

    click.echo("All download requests sent. Waiting for SCP to receive files...")
    
    # Give some time for files to be received. A more robust solution would be to
    # monitor the SCP thread or check for incoming files.
    time.sleep(10)
    
    # The SCP runs in a daemon thread, so it will exit when the main thread exits.
    # We could implement a more graceful shutdown.
    click.echo("Download process finished.")
