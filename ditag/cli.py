import click
import os
from . import config
from . import indexer
from . import querier
from . import sender

@click.group()
@click.option('--config-file', type=click.Path(), default=config.DEFAULT_CONFIG_FILE, help='Path to config file.')
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
@click.option('--archive', required=True, type=click.Path(exists=True, file_okay=False), help='Path to DICOM archive directory.')
@click.option('--append', is_flag=True, help='Append to existing database.')
@click.pass_context
def index(ctx, archive, append):
    """Index a DICOM archive."""
    cfg = ctx.obj['config']
    db_path = cfg['DEFAULT']['database']
    
    # Ensure the directory for the database exists
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        
    cfg['DEFAULT']['archive_path'] = archive
    
    indexer.index_archive(archive, db_path, append)
    config.save_config(cfg, ctx.obj['config_file'])
    click.echo(f"Configuration saved to {ctx.obj['config_file']}")


@cli.command()
@click.option('--sdate', help='Start date (YYYYMMDD).')
@click.option('--edate', help='End date (YYYYMMDD).')
@click.option('--date', help='Specific date (YYYYMMDD).')
@click.option('--targets', help='Comma-separated list of tags to search (e.g., SeriesDescription,StudyDescription).')
@click.option('--pattern', help='Regular expression pattern to search.')
@click.option('--output', type=click.Path(), help='Output file for results (CSV).')
@click.pass_context
def query(ctx, sdate, edate, date, targets, pattern, output):
    """Query the DICOM index."""
    cfg = ctx.obj['config']
    db_path = cfg['DEFAULT']['database']
    
    if targets:
        targets = targets.split(',')

    querier.query_db(db_path, sdate, edate, date, targets, pattern, output)


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
