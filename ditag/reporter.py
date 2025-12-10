import click
from . import project
from . import database
import os

def generate_report(project_name, get_cost=None):
    """Generates a summary report for a project."""
    proj_config = project.get_project_config(project_name)
    if not proj_config:
        click.echo(f"Project '{project_name}' not found.")
        return

    db_path = proj_config['database_path']
    if not os.path.exists(db_path):
        click.echo(f"Database for project '{project_name}' not found.")
        return

    conn = database.get_db_connection(db_path)
    cursor = conn.cursor()

    # Number of imaging sessions (studies)
    cursor.execute('SELECT COUNT(DISTINCT StudyInstanceUID) FROM series')
    num_studies = cursor.fetchone()[0]

    # Number of accession numbers provided
    with open(proj_config['target_list'], 'r') as f:
        num_accessions = len([line.strip() for line in f if line.strip()])

    # Number of unique PatientIDs
    cursor.execute('SELECT COUNT(DISTINCT PatientID) FROM series')
    num_patients = cursor.fetchone()[0]

    # Top 10 SeriesDescriptions
    cursor.execute('''
        SELECT SeriesDescription, COUNT(*) as count
        FROM series
        GROUP BY SeriesDescription
        ORDER BY count DESC
        LIMIT 10
    ''')
    top_series = cursor.fetchall()

    conn.close()

    # Cost estimation
    if get_cost is not None:
        proj_config['cost_per_study'] = get_cost
        project.save_project_config(project_name, proj_config)
    
    cost_per_study = proj_config.get('cost_per_study')
    estimated_cost = None
    if cost_per_study is not None:
        estimated_cost = num_studies * cost_per_study

    # Generate report content
    report_lines = []
    report_lines.append(f"Report for project: {project_name}")
    report_lines.append("=" * 30)
    report_lines.append(f"Imaging Sessions Located: {num_studies} / {num_accessions}")
    report_lines.append(f"Unique Patient IDs: {num_patients}")
    report_lines.append("\nTop 10 Series Descriptions:")
    for desc, count in top_series:
        report_lines.append(f"  - {desc}: {count}")

    if estimated_cost is not None:
        report_lines.append("\nEstimated Download Cost:")
        report_lines.append(f"  - Cost per study: ${cost_per_study:.2f}")
        report_lines.append(f"  - Total estimated cost: ${estimated_cost:.2f}")
    
    report_content = "\n".join(report_lines)

    # Output to stdout
    click.echo(report_content)

    # Save to file
    report_file = os.path.join(project.get_project_dir(project_name), f"{project_name}_report.txt")
    with open(report_file, 'w') as f:
        f.write(report_content)
    click.echo(f"\nReport saved to {report_file}")
