import click
import os
from . import config
from . import indexer
from . import querier
from . import sender
from . import project
from . import reporter
from . import downloader

class NaturalOrderGroup(click.Group):
    """A group that lists commands in the order they are added."""
    def list_commands(self, ctx):
        return list(self.commands)

@click.group(cls=NaturalOrderGroup)
@click.option('--config-file', type=click.Path(), default=config.DEFAULT_MAIN_CONFIG_FILE, help='Path to config file.')
@click.pass_context
def cli(ctx, config_file):
    """A CLI tool for indexing, querying, and sending DICOM files."""
    ctx.ensure_object(dict)
    if os.path.exists(config_file):
        ctx.obj['config'] = config.get_config(config_file)
    else:
        ctx.obj['config'] = config.get_default_config()
    ctx.obj['config_file'] = config_file

@cli.command()
@click.option('--archive', type=click.Path(exists=True, file_okay=False), help='Path to DICOM archive directory.')
@click.option('--append', is_flag=True, help='Append to existing database.')
@click.option('--threads', default=4, type=int, help='Number of threads to use for indexing.')
@click.pass_context
def index(ctx, archive, append, threads):
    """Index a DICOM archive."""
    cfg = ctx.obj['config']
    db_path = cfg['DEFAULT']['database']
    
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        
    cfg['DEFAULT']['archive_path'] = archive
    
    indexer.index_archive(archive, db_path, append, threads)
    config.save_config(cfg, ctx.obj['config_file'])
    click.echo(f"Configuration saved to {ctx.obj['config_file']}")

@cli.group(cls=NaturalOrderGroup, name='project')
def project_group():
    """Commands for managing projects."""
    pass

@project_group.command(name='index')
@click.option('--project-name', required=True, help='Project name for PACS indexing.')
@click.option('--pacs', required=True, help='PACS IP or hostname for project-based indexing.')
@click.option('--port', required=True, type=int, help='PACS port for project-based indexing.')
@click.option('--aetitle', required=True, help='PACS AE title for project-based indexing.')
@click.option('--target-list', required=True, type=click.Path(exists=True, dir_okay=False), help='Text file with list of accession numbers for project-based indexing.')
@click.option('--start-at-line', type=int, help='Line number to start indexing from.')
@click.option('--start-at-accession', help='Accession number to start indexing from.')
@click.pass_context
def project_index(ctx, project_name, pacs, port, aetitle, target_list, start_at_line, start_at_accession):
    """Index a project from PACS."""
    if start_at_line and start_at_accession:
        raise click.UsageError('Cannot use --start-at-line and --start-at-accession at the same time.')
    
    click.echo(f"Creating project '{project_name}'...")
    proj_config = project.create_project(project_name, pacs, port, aetitle, target_list)

    proj_config['start_at_line'] = start_at_line
    proj_config['start_at_accession'] = start_at_accession
    
    click.echo(f"Indexing project '{project_name}' from PACS...")
    indexer.index_pacs(proj_config)
    click.echo("Project indexing complete.")

@project_group.command(name='report')
@click.option('--project-name', required=True, help='Project name to generate report for.')
@click.option('--get-cost', type=float, help='Set cost per study and include in report.')
def project_report(project_name, get_cost):
    """Generate a summary report for a project."""
    reporter.generate_report(project_name, get_cost)

@cli.command()
@click.option('--sdate', help='Start date (YYYYMMDD).')
@click.option('--edate', help='End date (YYYYMMDD).')
@click.option('--date', help='Specific date (YYYYMMDD).')
@click.option('--targets', help='Comma-separated list of tags to search (e.g., SeriesDescription,StudyDescription).')
@click.option('--pattern', help='Regular expression pattern to search.')
@click.option('--output', type=click.Path(), help='Output file for results (CSV).')
@click.option('--project-name', help='Project name to query.')
@click.pass_context
def query(ctx, sdate, edate, date, targets, pattern, output, project_name):
    """Query the DICOM index."""
    if project_name:
        db_path = project.get_project_db_path(project_name)
    else:
        cfg = ctx.obj['config']
        db_path = cfg['DEFAULT']['database']
    
    if targets:
        targets = targets.split(',')

    querier.query_db(db_path, sdate, edate, date, targets, pattern, output)

project_group.add_command(query)

@project_group.command(name='download')
@click.option('--project-name', required=True, help='Project name to download.')
@click.option('--threads', default=4, type=int, help='Number of threads for downloading.')
@click.option('--output', required=True, type=click.Path(file_okay=False), help='Output directory for downloaded files.')
@click.option('--my-aet', default='DITAG', help='My AE Title for the SCP.')
@click.option('--scp-port', default=11112, type=int, help='Port for the SCP.')
@click.option('--input', type=click.Path(exists=True, dir_okay=False), help='Input file with series to download (CSV).')
@click.option('--zip-project', is_flag=True, help='Zip the downloaded files by subject.')
def project_download(project_name, threads, output, my_aet, scp_port, input, zip_project):
    """Download data for a project from PACS."""
    downloader.download_project(project_name, threads, output, my_aet, scp_port, zip_project, input)

@cli.command()
@click.option('--destination', help='PACS destination address.')
@click.option('--port', help='PACS port number.')
@click.option('--pacs-aetitle', help='PACS AE Title.')
@click.option('--myaet', help='My AE Title (default: DITAG).')
@click.option('--input', type=click.Path(exists=True, dir_okay=False), help='Input file with series to send (CSV).')
@click.pass_context
def send(ctx, destination, port, pacs_aetitle, myaet, input):
    """Send DICOM files to a PACS destination."""
    cfg = ctx.obj['config']
    db_path = cfg['DEFAULT']['database']
    
    dest = destination or cfg.get('PACS', 'destination', fallback=None)
    port_str = str(port) if port else cfg.get('PACS', 'port', fallback=None)
    pacs_aet = pacs_aetitle or cfg.get('PACS', 'aetitle', fallback=None)
    my_aet = myaet or cfg.get('PACS', 'myaet', fallback='DITAG')

    p = None
    if port_str:
        try:
            p = int(port_str)
        except (ValueError, TypeError):
            click.echo(f"Error: Invalid port value '{port_str}'. Port must be an integer.")
            return
            
    if not all([dest, p, pacs_aet]):
        click.echo("Error: PACS destination, port, and AE title must be provided either via CLI options or in the config file.")
        return

    if 'PACS' not in cfg:
        cfg['PACS'] = {}
        
    cfg['PACS']['destination'] = dest
    cfg['PACS']['port'] = str(p)
    cfg['PACS']['aetitle'] = pacs_aet
    cfg['PACS']['myaet'] = my_aet

    sender.send_dicoms(db_path, my_aet, pacs_aet, dest, p, input)
    config.save_config(cfg, ctx.obj['config_file'])
    click.echo(f"Configuration updated and saved to {ctx.obj['config_file']}")

if __name__ == '__main__':
    cli()
