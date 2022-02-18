import threading
import time

import click
from pydantic import BaseModel
import rich
from rich.live import Live
from rich.table import Table

from pipelime.pipes.communication import PiperCommunicationChannelFactory


def percentage_string(v: float):
    color = "red"
    if v > 0.33:
        color = "yellow"
    if v > 0.66:
        color = "green"
    return f"[{color}]{v*100:.1f}%[/{color}]"


def generate_table(tasks_map: dict) -> Table:
    """Make a new table."""
    table = Table()
    table.add_column("File")
    table.add_column("Method")
    table.add_column("ID")
    table.add_column("Progress")

    for task_id in tasks_map.keys():
        progress = [percentage_string(v) for _, v in tasks_map[task_id].items()]
        progress = " ‖ ".join(progress)

        filename, method, unique = task_id.split(":")
        table.add_row(
            f"{filename}",
            f"{method}",
            f"{unique}",
            f"{progress}",
        )

    return table


class ChunkProgress(BaseModel):
    id: str
    chunk_index: int
    progress: float

    @property
    def file(self):
        return self.id.split(":")[0]

    @property
    def method(self):
        return self.id.split(":")[1]

    @property
    def unique(self):
        return self.id.split(":")[2]


@click.command("piper_watcher")
@click.option("-t", "--token", default="", help="Token to use for the piper.")
def piper_watcher(token: str):

    tasks_map = {}

    def callback(data: dict):
        if "_progress" in data["payload"]:
            chunk_progress = ChunkProgress(
                id=data["id"],
                chunk_index=data["payload"]["_progress"]["chunk_index"],
                progress=data["payload"]["_progress"]["progress_data"]["advance"]
                / data["payload"]["_progress"]["progress_data"]["total"],
            )

            if chunk_progress.id not in tasks_map:
                tasks_map[chunk_progress.id] = {}
            if chunk_progress.chunk_index not in tasks_map[chunk_progress.id]:
                tasks_map[chunk_progress.id][chunk_progress.chunk_index] = 0.0

            tasks_map[chunk_progress.id][
                chunk_progress.chunk_index
            ] += chunk_progress.progress
        elif "event" in data["payload"]:
            event = data["payload"]["event"]

            if event not in tasks_map:
                tasks_map[event] = {0: 0.0}

            tasks_map[event][0] += 1

    def listener_thread():
        channel = PiperCommunicationChannelFactory.create_channel(token)
        rich.print(f"Channel class: {channel.__class__}")
        channel.register_callback(callback)
        channel.listen()

    thread = threading.Thread(target=listener_thread, daemon=True)
    thread.start()

    with Live(generate_table(tasks_map), refresh_per_second=4) as live:
        while True:
            time.sleep(0.1)
            live.update(generate_table(tasks_map))

    thread.join()


if __name__ == "__main__":
    piper_watcher()
