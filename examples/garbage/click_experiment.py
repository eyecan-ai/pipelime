from os import name
from typing import List
import click

import click


class CLICommandInvoker(click.Command):

    # def parse_args(self, ctx: click.Context, args: List[str]) -> List[str]:
    #     print("PARSING", args)
    #     return super().parse_args(ctx, args)

    def invoke(self, ctx: click.Context):
        command = ctx.command.name
        print("Invoking ", ctx)

        ctx.command = click.option("-p", "--pino", required=True, help="HELLOE")(
            ctx.command
        )
        ret = super(CLICommandInvoker, self).invoke(ctx)
        return ret


@click.command("ciao", help="This is command", cls=CLICommandInvoker)
@click.option("-o", "--mouse")
def ciao(**kwargs):
    print("Done", kwargs)


if __name__ == "__main__":
    ciao()
