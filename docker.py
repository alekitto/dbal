import subprocess
import sys
import time
from os import path

# base dir of workspace
dir_workspace = path.dirname(path.realpath(__file__))

# start database server and return a URL to use to connect
def start_database(driver, database):
    if driver == "sqlite":
        # short-circuit for sqlite
        return f"sqlite://{database}"

    res = subprocess.run(
        ["docker", "compose", "up", "-d", driver],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=dir_workspace,
    )

    if res.returncode != 0:
        print(res.stderr, file=sys.stderr)

    if b"done" in res.stderr:
        time.sleep(30)

    # determine appropriate port for driver
    if driver.startswith("mysql") or driver.startswith("mariadb"):
        port = 3306

    elif driver.startswith("postgres"):
        port = 5432

    elif driver.startswith("mssql"):
        port = 1433

    else:
        raise NotImplementedError

    # find port
    res = subprocess.run(
        ["docker", "inspect", f"-f='{{{{(index (index .NetworkSettings.Ports \"{port}/tcp\") 0).HostPort}}}}'",
         f"dbal-{driver}-1"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=dir_workspace,
    )

    if res.returncode != 0:
        print(res.stderr, file=sys.stderr)

    port = int(res.stdout[1:-2].decode())

    # construct appropriate database URL
    if driver.startswith("mysql") or driver.startswith("mariadb"):
        return f"mysql://root:password@127.0.0.1:{port}/{database}"

    elif driver.startswith("postgres"):
        return f"postgres://postgres:password@localhost:{port}/{database}"

    elif driver.startswith("mssql"):
        return f"mssql://sa:Password123!@127.0.0.1:{port}/{database}"

    else:
        raise NotImplementedError
