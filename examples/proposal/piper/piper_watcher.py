from choixe.bulletins import BulletinBoard, Bulletin
import click
import rich


@click.command("piper_watcher")
@click.option("-t", "--token", default="", help="Token to use for the piper.")
def piper_watcher(token: str):
    def bulletin_update(bulletin: Bulletin):
        rich.print("New bulletin received:")
        rich.print(bulletin.metadata)

    main_board = BulletinBoard(session_id=token)
    main_board.register_callback(bulletin_update)
    main_board.wait_for_bulletins()


if __name__ == "__main__":
    piper_watcher()
