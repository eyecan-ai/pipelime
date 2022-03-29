import time

import click

from pipelime.pipes.watcher import Watcher


@click.command("piper_watcher")
@click.option("-t", "--token", default="", help="Token to use for the piper.")
def piper_watcher(token: str):
    Watcher(token).watch()
    while True:
        time.sleep(1)


if __name__ == "__main__":
    piper_watcher()
