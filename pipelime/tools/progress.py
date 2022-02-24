from typing import Callable, Iterable, List, Sequence, Optional, Any, Sized, Union
import warnings
from rich.console import Console
from rich.progress import (
    Progress,
    ProgressColumn,
    TextColumn,
    BarColumn,
    TimeRemainingColumn,
    ProgressType,
    _TrackThread,
    TaskID,
)


class PipelimeProgress(Progress):
    def __init__(
        self,
        *columns: Union[str, ProgressColumn],
        console: Optional[Console] = None,
        auto_refresh: bool = False,
        refresh_per_second: float = 10,
        speed_estimate_period: float = 30,
        transient: bool = False,
        redirect_stdout: bool = True,
        redirect_stderr: bool = True,
        get_time: Optional[Any] = None,
        disable: bool = False,
        expand: bool = False,
        style: str = "bar.back",
        complete_style: str = "bar.complete",
        finished_style: str = "bar.finished",
        pulse_style: str = "bar.pulse",
        track_callback: Optional[Callable[[Any], None]] = None,
    ) -> None:
        """Simple wrapper for rich progress bar. It adds a user callback to track progress."""

        self._track_callback = track_callback

        if auto_refresh:
            msg = """Using auto_refresh = True can result in deadlock if an exception 
            is raised while tracking progress, you should consider disabling 
            uto_refresh"""
            warnings.warn(msg)

        super().__init__(
            *columns,
            auto_refresh=auto_refresh,
            console=console,
            transient=transient,
            get_time=get_time,
            refresh_per_second=refresh_per_second or 10,
            disable=disable,
        )

    def _manage_track_callback(
        self, task_id: Optional[TaskID] = None, total: float = 1.0, advance: float = 1.0
    ):
        if self._track_callback is not None:
            self._track_callback(
                {"task_id": task_id, "advance": advance, "total": total}
            )

    def track(
        self,
        sequence: Union[Iterable[ProgressType], Sequence[ProgressType]],
        total: Optional[float] = None,
        task_id: Optional[TaskID] = None,
        description: str = "Working...",
        update_period: float = 0.1,
    ) -> Iterable[ProgressType]:
        """Track progress by iterating over a sequence.

        Args:
            sequence (Sequence[ProgressType]): A sequence of values you want to iterate over and track progress.
            total: (float, optional): Total number of steps. Default is len(sequence).
            task_id: (TaskID): Task to track. Default is new task.
            description: (str, optional): Description of task, if new task is created.
            update_period (float, optional): Minimum time (in seconds) between calls to update(). Defaults to 0.1.

        Returns:
            Iterable[ProgressType]: An iterable of values taken from the provided sequence.
        """

        if total is None:
            if isinstance(sequence, Sized):
                task_total = float(len(sequence))
            else:
                raise ValueError(
                    f"unable to get size of {sequence!r}, please specify 'total'"
                )
        else:
            task_total = total

        if task_id is None:
            task_id = self.add_task(description, total=task_total)
        else:
            self.update(task_id, total=task_total)

        if self.live.auto_refresh:
            with _TrackThread(self, task_id, update_period) as track_thread:
                for value in sequence:
                    yield value
                    self._manage_track_callback(
                        task_id=task_id,
                        advance=1,
                        total=task_total,
                    )
                    track_thread.completed += 1
        else:
            advance = self.advance
            refresh = self.refresh
            for value in sequence:
                yield value
                self._manage_track_callback(
                    task_id=task_id,
                    advance=1,
                    total=task_total,
                )
                advance(task_id, 1)
                refresh()


def pipelime_track(
    sequence: Union[Sequence[ProgressType], Iterable[ProgressType]],
    description: str = "Working...",
    total: Optional[float] = None,
    auto_refresh: bool = False,  # Passing auto_refresh=True may results in deadlock
    console: Optional[Console] = None,
    transient: bool = False,
    get_time: Optional[Callable[[], float]] = None,
    refresh_per_second: float = 10,
    style: str = "bar.back",
    complete_style: str = "bar.complete",
    finished_style: str = "bar.finished",
    pulse_style: str = "bar.pulse",
    update_period: float = 0.1,
    disable: bool = False,
    track_callback: Optional[Callable[[Any], None]] = None,
) -> Iterable[ProgressType]:
    """Track progress by iterating over a sequence.

    Args:
        sequence (Iterable[ProgressType]): A sequence (must support "len") you wish to iterate over.
        description (str, optional): Description of task show next to progress bar. Defaults to "Working".
        total: (float, optional): Total number of steps. Default is len(sequence).
        auto_refresh (bool, optional): Automatic refresh, disable to force a refresh after each iteration. Default is True.
        transient: (bool, optional): Clear the progress on exit. Defaults to False.
        console (Console, optional): Console to write to. Default creates internal Console instance.
        refresh_per_second (float): Number of times per second to refresh the progress information. Defaults to 10.
        style (StyleType, optional): Style for the bar background. Defaults to "bar.back".
        complete_style (StyleType, optional): Style for the completed bar. Defaults to "bar.complete".
        finished_style (StyleType, optional): Style for a finished bar. Defaults to "bar.done".
        pulse_style (StyleType, optional): Style for pulsing bars. Defaults to "bar.pulse".
        update_period (float, optional): Minimum time (in seconds) between calls to update(). Defaults to 0.1.
        disable (bool, optional): Disable display of progress.
        track_callback: (Callable[[Any], None], optional): Callback to call with progress information.
    Returns:
        Iterable[ProgressType]: An iterable of the values in the sequence.

    """

    columns: List["ProgressColumn"] = (
        [TextColumn("🍋|[progress.description]{task.description}")]
        if description
        else [TextColumn("🍋|")]
    )
    columns.extend(
        (
            BarColumn(
                style=style,
                complete_style=complete_style,
                finished_style=finished_style,
                pulse_style=pulse_style,
            ),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
        )
    )
    progress = PipelimeProgress(
        *columns,
        auto_refresh=auto_refresh,
        console=console,
        transient=transient,
        get_time=get_time,
        refresh_per_second=refresh_per_second or 10,
        disable=disable,
        track_callback=track_callback,
    )

    with progress:
        yield from progress.track(
            sequence, total=total, description=description, update_period=update_period
        )
