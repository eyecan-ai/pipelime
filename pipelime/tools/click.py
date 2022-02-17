from re import X
import click


class ClickTools:
    @classmethod
    def parse_additional_args(cls, ctx: click.Context) -> dict:
        """Parse additional arguments from the click context. Usually they are parameters
        not defined in the click command but provided to CLI.

        Args:
            ctx (click.Context): click context

        Raises:
            click.UsageError: if arguments list is not even

        Returns:
            dict: additional arguments
        """

        # remove '--' and '-' from args names
        x = [x.strip("-") for x in ctx.args]

        # check even number of args
        if len(x) % 2 != 0:
            raise click.UsageError("Invalid number of arguments")

        # create dict from pairs key:value
        x = [x[i : i + 2] for i in range(0, len(x), 2)]

        return dict(x)
