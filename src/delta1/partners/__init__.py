"""Mock external partners: an FX-rates REST API and an FTP drop with game-provider reports.

In production these are third parties. Here they are simulated so the Airflow DAGs have
something real to pull from, including the failure modes that make partner ingestion hard:
flaky HTTP, files that arrive late, and provider numbers that do not match ours.
"""
