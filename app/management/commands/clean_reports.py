import click
from app.models.report_cache import ReportCache

def register(app):
    @app.cli.command("clean-reports")
    def clean_reports():
        """
        Manually trigger the cleanup of expired reports from the cache.
        """
        try:
            with app.app_context():
                count = ReportCache.cleanup_expired()
                click.echo(f"Successfully deleted {count} expired report(s).")
        except Exception as e:
            click.echo(f"An error occurred during report cleanup: {e}", err=True)