"""Runs the mock partner: REST API, FTP server and the report writer."""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path

import uvicorn
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

from delta1.config import ClickHouseSettings
from delta1.partners.reports import ReportWriter
from delta1.processor.sink import connect

log = logging.getLogger("delta1.partners")


def ftp_server(root: Path, user: str, password: str, port: int) -> FTPServer:
    authorizer = DummyAuthorizer()
    authorizer.add_user(user, password, str(root), perm="elr")  # read-only
    handler = FTPHandler
    handler.authorizer = authorizer
    handler.passive_ports = range(30000, 30010)
    handler.banner = "delta1 provider reports"
    return FTPServer(("0.0.0.0", port), handler)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    root = Path(os.environ.get("REPORT_ROOT", "/data/reports"))
    root.mkdir(parents=True, exist_ok=True)
    window = int(os.environ.get("REPORT_WINDOW_MINUTES", "10"))

    writer = ReportWriter(connect(ClickHouseSettings()), root, window_minutes=window)
    threading.Thread(target=writer.run_forever, name="reports", daemon=True).start()

    ftp = ftp_server(
        root,
        os.environ.get("FTP_USER", "provider"),
        os.environ.get("FTP_PASSWORD", "provider"),
        int(os.environ.get("FTP_PORT", "2121")),
    )
    threading.Thread(target=ftp.serve_forever, name="ftp", daemon=True).start()
    log.info("reports every %d min in %s, FTP on :%s", window, root, ftp.address[1])

    uvicorn.run("delta1.partners.app:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))


if __name__ == "__main__":
    main()
