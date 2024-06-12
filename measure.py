import argparse
import asyncio
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import List, Optional

import aiosqlite
import matplotlib.pyplot as plt
import psutil
from tabulate import tabulate

schema_query = """
    CREATE TABLE IF NOT EXISTS events(
        id INTEGER PRIMARY KEY,
        proc_name TEXT NOT NULL,
        cpu_usage REAL NOT NULL,
        memory_usage REAL NOT NULL,
        uss REAL,
        read_bytes REAL,
        write_bytes REAL,
        charge_diff REAL DEFAULT 0,
        ts DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS builds(
        proc_name TEXT,
        build TEXT
    );
"""

write_event_query = """
    INSERT INTO events(
        proc_name,
        cpu_usage,
        memory_usage,
        uss,
        read_bytes,
        write_bytes,
        charge_diff,
        ts
    )
    VALUES(
        ?,?,?,?,?,?,?,?
    );
"""

process_query = """
    SELECT cpu_usage, memory_usage, uss, charge_diff
    FROM events
    WHERE proc_name = ?
    AND ts BETWEEN ? AND ?
    LIMIT ?
    OFFSET ?
"""

build_query = """SELECT cpu_usage, memory_usage, uss, charge_diff, ts
                FROM events
                JOIN builds ON events.proc_name=builds.proc_name
                WHERE builds.build = ?
                AND events.ts BETWEEN ? AND ?
                ORDER BY events.ts ASC
                LIMIT ?
                OFFSET ?
            """

count_query = """SELECT COUNT(*)
                FROM events
                WHERE proc_name = ?
                AND ts BETWEEN ? AND  ?
              """

class Actions(StrEnum):
    COLLECT = "collect"
    QUERY = "query"
    MATCH_BUILD = "match_build"
    QUERY_BUILD = "query_build"


class BuildBase(StrEnum):
    ELECTRON = "electron"
    JVM = "jvm"
    DOTNET = "dotnet"
    QT = "qt"
    OTHER = "?"


async def detect_tech_stack(
    proc_name: str, installation_path: Optional[str] = None
) -> BuildBase:
    """Automated way to detect build details -- Not Used"""  # noqa
    if not installation_path:
        tech = await prompt_for_build_tech(proc_name)
        return tech if isinstance(tech, BuildBase) else BuildBase.OTHER
    for child in Path(installation_path).iterdir():
        if child.is_file() and "asar" in child.suffix:
            return BuildBase.ELECTRON
        elif child.is_dir() and child.name == "jre":
            return BuildBase.JVM
        else:
            continue
    tech = await prompt_for_build_tech(proc_name)
    return tech


async def prompt_for_build_tech(proc_name: str) -> None:
    """Ask user to submit the tech stack used to build the app
    if automatic results are ambiguous
    """
    inp = input(
        f">> What is this process built with?\n>> Options: {'/'.join(BuildBase)}\n>>"
    )

    inp = inp.strip().lower()

    match (inp):
        case "jvm":
            return BuildBase.JVM
        case "electron":
            return BuildBase.ELECTRON
        case "dotnet":
            return BuildBase.DOTNET
        case "qt":
            return BuildBase.QT
        case _:
            return BuildBase.OTHER


async def save_build_info(proc_name: str) -> None:
    """Append build information to db"""
    build = await detect_tech_stack(proc_name)
    async with db_session() as db:
        name_exists = await db.execute(
            """
                       SELECT proc_name
                       FROM builds
                       WHERE proc_name = ?
                   """,
            [proc_name],
        )
        if await name_exists.fetchone():
            print(f"[X] Updating build information for process {proc_name}")
            try:
                await db.execute(
                    """
                        UPDATE builds
                        SET build = ?
                        WHERE proc_name = ?
                    """,
                    (build.value, proc_name),
                )
            except aiosqlite.OperationalError as e:
                print("[E] Error updating build information", e)
        else:
            try:
                await db.execute(
                    """
                      INSERT INTO builds
                      VALUES (?, ?)
                    """,
                    (proc_name, build.value),
                )
            except aiosqlite.OperationalError:
                print(f"[E] Error creating build information for {proc_name}")


async def query_build(display_in_browser: bool = False) -> None:
    """Query events from processes following the input build"""
    build = input(f">> Query build technology:\n Options: {'/'.join(BuildBase)}\n>>")

    ts_from = input(">> Query from datetime: \n>> Format -> (dd/mm/YYYY HH:MM:SS)\n>> ")

    ts_to = input(">> Query to datetime: \n>> Format -> (dd/mm/YYYY HH:MM:SS)\n>> ")

    limit = input(">> Set results limit: ")
    offset = input(">> Set results offset: ")

    try:
        datetime_to = datetime.strptime(ts_to, "%d/%m/%Y %H:%M:%S")
        datetime_to = datetime_to - datetime_to.astimezone().tzinfo.utcoffset(datetime_to)
        datetime_from = datetime.strptime(ts_from, "%d/%m/%Y %H:%M:%S")
        datetime_from = datetime_from - datetime_from.astimezone().tzinfo.utcoffset(datetime_from)
    except ValueError as e:
        print("[E] Date format Error\n\t", e)
        sys.exit(1)

    try:
        limit = int(limit)
        offset = int(offset)
    except ValueError:
        print("[E] Could not parse limit/offset")
        sys.exit(1)

    async with db_session() as db:
        rows = await db.execute(
            build_query,
            (build, datetime_from, datetime_to, limit, offset),
        )
        rows_result = await rows.fetchall()

    if display_in_browser:
        show_in_browser(rows_result)
    else:
        print(f"From: {datetime_from}, To: {datetime_to}")
        print(
            tabulate(
                [row for row in rows_result],
                ["CPU %", "Memory RSS (MB)", "Memory USS (MB)", "Charge Difference %"],
                tablefmt="pretty",
            )
        )


def get_main_process_by_name(process_name):
    """Get the main process matching the process_name"""
    main_process = None
    for proc in psutil.process_iter(["pid", "name", "create_time"]):
        try:
            if process_name.lower() in proc.info["name"].lower():
                if (
                    main_process is None
                    or proc.info["create_time"] < main_process.info["create_time"]
                ):
                    main_process = proc
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return main_process


def get_process_tree(root_process):
    """
    Get all child processes of the root_process recursively.
    """
    children = []
    try:
        children = root_process.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        pass
    return [root_process] + children


async def monitor_process_tree(process_tree, duration, event_q: asyncio.Queue):
    """
    Monitor the specified process tree for the given duration.
    TODO: Reduce event_q size -- Normalize db tables
    """
    print(f"Monitoring process tree for {duration} seconds")
    start_time = time.time()
    previous_read_bytes = 0
    previous_write_bytes = 0
    while time.time() - start_time < duration:
        total_cpu_usage = 0
        total_memory_usage = 0
        total_uss = 0
        total_read_bytes = 0
        total_write_bytes = 0
        initial_charge = 0

        battery = psutil.sensors_battery()
        if battery:
            initial_charge = battery.percent

        for proc in process_tree:
            try:
                total_cpu_usage += proc.cpu_percent(interval=None)
                # check uss available on platform
                # if not available, set None
                try:
                    memory_full_info = proc.memory_full_info()
                    total_memory_usage += memory_full_info.rss / (1024 * 1024)
                    total_uss += memory_full_info.uss / (1024 * 1024)
                except AttributeError:
                    total_uss = None

                io_counters = proc.io_counters()
                total_read_bytes += io_counters.read_bytes / (
                    1024 * 1024
                ) 
                total_write_bytes += io_counters.write_bytes / (
                    1024 * 1024
                ) 
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

        # disk operations
        read_bytes_diff = total_read_bytes - previous_read_bytes
        write_bytes_diff = total_write_bytes - previous_write_bytes
        previous_read_bytes = total_read_bytes
        previous_write_bytes = total_write_bytes

        # network operations

        # log battery usage
        if battery:
            charge_diff = initial_charge - battery.percent
        else:
            charge_diff = initial_charge

        event_q.put_nowait(
            (
                # root process name, different inner processes
                # contribute under the same name #noqa
                process_tree[0].name(),
                total_cpu_usage,
                total_memory_usage,
                total_uss,
                read_bytes_diff,
                write_bytes_diff,
                charge_diff,
                datetime.now(timezone.utc),
            )
        )

        await asyncio.sleep(0.85)


async def init_db():
    """Initialize db tables from script
    TODO: Normalize 1st-stage::processes
    """
    async with aiosqlite.connect("events.db") as db:
        try:
            await db.executescript(schema_query)
            await db.commit()
            await db.close()
        except aiosqlite.OperationalError as e:
            print("[E] Error in appling database schema\n\t", e)
            sys.exit(1)


@asynccontextmanager
async def db_session():
    """
    Generate db session and manage it's lifecycle
    """
    async with aiosqlite.connect("events.db") as db:
        try:
            yield db
        except aiosqlite.OperationalError as e:
            print("[E] Error in yielding database session:\n\t", e)
            sys.exit(1)
        finally:
            await db.commit()
            await db.close()


async def match_pid_to_process(pid: int) -> Optional[psutil.Process]:
    """Match process pid to process name"""
    if not psutil.pid_exists(pid):
        print(f"[E] No running process found with {pid=}")
        sys.exit(1)
    process = [
        proc for proc in psutil.process_iter(["pid", "name"]) if proc.info["pid"] == pid
    ]

    return process[0] if len(process) > 0 else None


async def track_process(
    main_process: Optional[psutil.Process], duration: int = 0, display: bool = False
) -> None:
    """Track the resources consumed by a process in a given time span.
    Duration in seconds
    """  # noqa

    if not main_process:
        print(f"No processes found for input")
        sys.exit(1)

    event_q = asyncio.Queue()

    print(f"Main process: {main_process.info['name']} (PID: {main_process.pid})")
    process_tree = get_process_tree(main_process)

    monitor_start_ts = datetime.now(timezone.utc)
    await monitor_process_tree(process_tree, duration, event_q)
    monitor_end_ts = datetime.now(timezone.utc)

    # ignore first element in fifo
    first_el = event_q.get_nowait()
    del first_el

    print("[X] Finished monitoring process tree, writing events to database...")

    async with db_session() as db:
        tasks = []
        while event_q.qsize() > 0:
            event = event_q.get_nowait()
            tasks.append(db.execute(write_event_query, event))
        async with asyncio.TaskGroup() as tg:
            tg_tasks = [tg.create_task(t) for t in tasks]  # noqa

    print("[X] Finished writing events to database")
    if display:
        print("[X] Displaying Sample Data [0:10]")
        await display_query(main_process, monitor_start_ts, monitor_end_ts)


async def query_process(proc_name: str) -> None:
    """Query collected porcesses from db"""
    ts_from = input(">> Query from datetime: \n>> Format -> (dd/mm/YYYY HH:MM:SS)\n>> ")

    ts_to = input(">> Query to datetime: \n>> Format -> (dd/mm/YYYY HH:MM:SS)\n>> ")

    limit = input(">> Set results limit: ")
    offset = input(">> Set results offset: ")

    try:
        datetime_to = datetime.strptime(ts_to, "%d/%m/%Y %H:%M:%S")
        datetime_to = datetime_to - datetime_to.astimezone().tzinfo.utcoffset(datetime_to)
        datetime_from = datetime.strptime(ts_from, "%d/%m/%Y %H:%M:%S")
        datetime_from = datetime_from - datetime_from.astimezone().tzinfo.utcoffset(datetime_from)
    except ValueError as e:
        print("[E] Date format Error\n\t", e)
        sys.exit(1)

    try:
        limit = int(limit)
        offset = int(offset)
    except ValueError:
        print("[E] Could not parse limit/offset")
        sys.exit(1)

    async with db_session() as db:
        num_rows = await db.execute(
            count_query,
            (proc_name, datetime_from, datetime_to),
        )
        print(f"[X] Total Rows: {(await num_rows.fetchone())[0]}")
        print(f"From: {datetime_from}, To: {datetime_to}")

        rows = await db.execute(
            process_query,
            (proc_name, datetime_from, datetime_to, limit, offset),
        )
        rows_result = await rows.fetchall()

    print(
        tabulate(
            [row for row in rows_result],
            ["CPU %", "Memory RSS (MB)", "Memory USS (MB)", "Charge Difference %"],
            tablefmt="pretty",
        )
    )


async def display_query(proc_name, ts_from, ts_to, limit=10, offset=0):

    print(f"Process: {proc_name}")
    async with db_session() as db:
        rows = await db.execute(
            """SELECT cpu_usage, memory_usage, uss, charge_diff
               FROM events
               WHERE ts BETWEEN ? AND ?
               ORDER BY ts ASC
               LIMIT ?
               OFFSET ?
            """,
            (ts_from, ts_to, limit, offset),
        )
        tabulate_header = [
            "CPU %",
            "Memory RSS (MB)",
            "Memory USS (MB)",
            "Charge Difference %",
        ]
        tabulate_data = [row for row in await rows.fetchall()]

    print(tabulate(tabulate_data, headers=tabulate_header, tablefmt="pretty"))


def show_in_browser(rows: List[str]) -> None:
    """Not Implemented
    - Produce plot
    - save to BytesIO
    - stream as base64 image over websocket
    """
    cpu, rss, uss, batt, ts = zip(*rows)
    plt.figure(figsize=(10, 6), layout="constrained")
    plt.title("CPU % - Time\nElectron 8/6 -> 11/6 500 samples")
    plt.plot(ts, cpu)
    plt.xlabel("Time")
    plt.ylabel("CPU %")
    plt.savefig("cpu.png")
    plt.show()


async def main() -> None:
    """Main function, choosing from a preset of actions to execute
    Available Actions:
    collect: arguments=(process, --is-pid, --duration)
    query: arguments=(process, --is-pid)
    match_build: arguments=(process, --is-pid)
    query_build: arguments=()
    """
    parser = argparse.ArgumentParser(
        description="Track resource consumption of a specific process tree"
    )
    parser.add_argument(
        "process", help="Name/PID of the process to monitor", nargs="?", default=""
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Duration to monitor the process tree (in seconds).\nUsed with collect action",  # noqa
    )
    parser.add_argument(
        "--is-pid",
        type=bool,
        default=False,
        help="Specify if process passed is given by name or pid, as found on system's task manager.",  # noqa
    )
    parser.add_argument("--action", type=Actions, default=Actions.COLLECT)
    args = parser.parse_args()

    if not args.process and args.action != Actions.QUERY_BUILD:
        print("[E] Please enter a valid process name/id for the chosen action")
        sys.exit(1)

    if args.is_pid:
        try:
            process_name = await match_pid_to_process(int(args.process))
            main_process = process_name
            offline_proc_name = main_process.info["name"]
        except ValueError:
            print("PID passed cannot be converted to an integer")
            sys.exit(1)
    else:
        process_name = args.process
        main_process = get_main_process_by_name(process_name)
        offline_proc_name = process_name

    duration = args.duration

    await init_db()

    if args.action == Actions.COLLECT.value:
        if not duration or not isinstance(duration, int):
            print("[E] Please enter duration using --duration")
            sys.exit(1)
        await track_process(main_process, duration, display=True)
    elif args.action == Actions.QUERY.value:
        await query_process(offline_proc_name)
    elif args.action == Actions.MATCH_BUILD.value:
        await save_build_info(offline_proc_name)
    elif args.action == Actions.QUERY_BUILD.value:
        await query_build(display_in_browser=False)
    else:
        print("[E] Action not available")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
