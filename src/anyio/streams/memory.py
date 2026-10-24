from __future__ import annotations

__all__ = (
    "BlockedSendSurfacePolicy",
    "BufferLedgerPolicy",
    "ChannelRefcountShutdownPolicy",
    "MemoryObjectReceiveStream",
    "MemoryObjectSendStream",
    "MemoryObjectStreamPolicy",
    "MemoryObjectStreamStatistics",
)

import warnings
from collections import OrderedDict, deque
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Generic, NamedTuple, TypeVar

from .._core._exceptions import (
    BrokenResourceError,
    ClosedResourceError,
    EndOfStream,
    NoEventLoopError,
    WouldBlock,
)
from .._core._synchronization import Event
from .._core._testing import TaskInfo, get_current_task, get_running_tasks
from ..abc import ObjectReceiveStream, ObjectSendStream
from ..lowlevel import RunVar, checkpoint

T_Item = TypeVar("T_Item")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


@dataclass(frozen=True)
class BufferLedgerPolicy:
    """Policy for how items are accounted for against the stream's steady capacity."""

    steady_cap: bool = True
    allow_receive_staging: bool = True
    forbid_staging_overshoot: bool = False

    def __post_init__(self) -> None:
        if not self.steady_cap:
            raise ValueError("only the create-time steady capacity is supported")

        if self.allow_receive_staging == self.forbid_staging_overshoot:
            raise ValueError(
                "exactly one of allow_receive_staging and forbid_staging_overshoot "
                "must be enabled"
            )


@dataclass(frozen=True)
class BlockedSendSurfacePolicy:
    """Policy for how a full buffer is presented to a blocked sender."""

    queue_waiters: bool = True
    fail_fast: bool = False
    reclaim_on_send_cancel: bool = False

    def __post_init__(self) -> None:
        if self.queue_waiters == self.fail_fast:
            raise ValueError(
                "exactly one of queue_waiters and fail_fast must be enabled"
            )


@dataclass(frozen=True)
class ChannelRefcountShutdownPolicy:
    """Pin-compatible policy for refcount based half-closing."""

    last_receiver_breaks_senders: bool = True
    last_sender_ends_receivers: bool = True
    clone_partial_keep_blocked: bool = True

    def __post_init__(self) -> None:
        if not (
            self.last_receiver_breaks_senders
            and self.last_sender_ends_receivers
            and self.clone_partial_keep_blocked
        ):
            raise ValueError("only the pin-compatible refcount shutdown is supported")


@dataclass(frozen=True)
class MemoryObjectStreamPolicy:
    """Effective memory object stream policy for a task."""

    buffer_ledger: BufferLedgerPolicy
    blocked_send_surface: BlockedSendSurfacePolicy
    channel_refcount_shutdown: ChannelRefcountShutdownPolicy


_DEFAULT_BUFFER_LEDGER = BufferLedgerPolicy()
_DEFAULT_BLOCKED_SEND_SURFACE = BlockedSendSurfacePolicy()
_DEFAULT_CHANNEL_REFCOUNT_SHUTDOWN = ChannelRefcountShutdownPolicy()
_DEFAULT_POLICY = MemoryObjectStreamPolicy(
    _DEFAULT_BUFFER_LEDGER,
    _DEFAULT_BLOCKED_SEND_SURFACE,
    _DEFAULT_CHANNEL_REFCOUNT_SHUTDOWN,
)


@dataclass(frozen=True)
class _TaskTreeAnchor:
    root_id: int
    ancestor_ids: frozenset[int]


@dataclass(frozen=True)
class _PolicyFrame:
    buffer_ledger: BufferLedgerPolicy | None
    blocked_send_surface: BlockedSendSurfacePolicy | None
    channel_refcount_shutdown: ChannelRefcountShutdownPolicy | None


@dataclass(eq=False)
class _ActivePolicyFrame:
    policy: _PolicyFrame
    owner_id: int


@dataclass(eq=False)
class _RootPolicyState:
    frames: list[_ActivePolicyFrame] = field(default_factory=list)


_memory_object_stream_policies: RunVar[dict[int, _RootPolicyState] | None] = RunVar(
    "_memory_object_stream_policies", None
)


def _task_tree_anchor(
    owner_id: int,
    tasks_by_id: dict[int, TaskInfo],
) -> _TaskTreeAnchor | None:
    task = tasks_by_id.get(owner_id)
    if task is None:
        return None

    ancestor_ids: set[int] = set()
    while True:
        if task.id in ancestor_ids:
            break

        ancestor_ids.add(task.id)
        if task.parent_id is None:
            root_id = task.id
            break

        parent = tasks_by_id.get(task.parent_id)
        if parent is None:
            root_id = task.id
            break

        task = parent

    return _TaskTreeAnchor(root_id, frozenset(ancestor_ids))


def _current_task_tree_anchor() -> _TaskTreeAnchor | None:
    try:
        current_task = get_current_task()
        running_tasks = get_running_tasks()
    except (NoEventLoopError, RuntimeError):
        # Trio raises RuntimeError when its synchronous test runner closes a stream
        # outside of the guest task.
        return None

    tasks_by_id = {task.id: task for task in running_tasks}
    tasks_by_id.setdefault(current_task.id, current_task)
    return _task_tree_anchor(current_task.id, tasks_by_id)


def _running_task_tree_anchor(owner_id: int) -> _TaskTreeAnchor | None:
    try:
        running_tasks = get_running_tasks()
    except (NoEventLoopError, RuntimeError):
        return None

    tasks_by_id = {task.id: task for task in running_tasks}
    return _task_tree_anchor(owner_id, tasks_by_id)


def _existing_root_policy_states() -> dict[int, _RootPolicyState] | None:
    try:
        return _memory_object_stream_policies.get()
    except NoEventLoopError:
        return None


def _ensure_root_policy_states() -> dict[int, _RootPolicyState]:
    states = _existing_root_policy_states()
    if states is None:
        states = {}
        _memory_object_stream_policies.set(states)

    return states


def _get_effective_policy(
    anchor: _TaskTreeAnchor | None = None,
) -> MemoryObjectStreamPolicy:
    states = _existing_root_policy_states()
    if not states:
        return _DEFAULT_POLICY

    if anchor is None:
        anchor = _current_task_tree_anchor()
    if anchor is None:
        return _DEFAULT_POLICY

    root_state = states.get(anchor.root_id)
    if root_state is None:
        return _DEFAULT_POLICY

    buffer_ledger = _DEFAULT_BUFFER_LEDGER
    blocked_send_surface = _DEFAULT_BLOCKED_SEND_SURFACE
    channel_refcount_shutdown = _DEFAULT_CHANNEL_REFCOUNT_SHUTDOWN
    for frame in root_state.frames:
        if frame.owner_id not in anchor.ancestor_ids:
            continue

        if frame.policy.buffer_ledger is not None:
            buffer_ledger = frame.policy.buffer_ledger
        if frame.policy.blocked_send_surface is not None:
            blocked_send_surface = frame.policy.blocked_send_surface
        if frame.policy.channel_refcount_shutdown is not None:
            channel_refcount_shutdown = frame.policy.channel_refcount_shutdown

    return MemoryObjectStreamPolicy(
        buffer_ledger,
        blocked_send_surface,
        channel_refcount_shutdown,
    )


@contextmanager
def memory_object_stream_policy(
    *,
    buffer_ledger: BufferLedgerPolicy | None = None,
    blocked_send_surface: BlockedSendSurfacePolicy | None = None,
    channel_refcount_shutdown: ChannelRefcountShutdownPolicy | None = None,
) -> Iterator[MemoryObjectStreamPolicy]:
    """
    Push a memory object stream policy frame for the current task and its descendants.

    The frame is automatically popped when the context block exits, including when the
    exit is caused by an exception or cancellation.

    :param buffer_ledger: overrides buffer/staging behavior for this frame
    :param blocked_send_surface: overrides full-buffer send behavior for this frame
    :param channel_refcount_shutdown: overrides half-closing behavior for this frame
    :return: a context manager that yields the effective merged policy
    :raises ~anyio.NoEventLoopError: if called outside a running event loop

    """
    anchor = _current_task_tree_anchor()
    if anchor is None:
        raise NoEventLoopError("an event loop is required to push a stream policy")

    states = _ensure_root_policy_states()
    root_state = states.setdefault(anchor.root_id, _RootPolicyState())
    frame = _ActivePolicyFrame(
        _PolicyFrame(
            buffer_ledger,
            blocked_send_surface,
            channel_refcount_shutdown,
        ),
        get_current_task().id,
    )
    root_state.frames.append(frame)
    try:
        yield _get_effective_policy(anchor)
    finally:
        root_state.frames.remove(frame)
        if not root_state.frames:
            states.pop(anchor.root_id, None)


# Keep the documented public location stable regardless of lazy-import order.
memory_object_stream_policy.__module__ = "anyio"


class MemoryObjectStreamStatistics(NamedTuple):
    current_buffer_used: int  #: number of items stored in the buffer
    #: maximum number of items that can be stored on this stream (or :data:`math.inf`)
    max_buffer_size: float
    open_send_streams: int  #: number of unclosed clones of the send stream
    open_receive_streams: int  #: number of unclosed clones of the receive stream
    #: number of tasks blocked on :meth:`MemoryObjectSendStream.send`
    tasks_waiting_send: int
    #: number of tasks blocked on :meth:`MemoryObjectReceiveStream.receive`
    tasks_waiting_receive: int


@dataclass(eq=False)
class _MemoryObjectItemReceiver(Generic[T_Item]):
    task_info: TaskInfo = field(init=False, default_factory=get_current_task)
    item: T_Item = field(init=False)
    end_of_stream: bool = field(init=False, default=False)

    def __repr__(self) -> str:
        # When item is not defined, we get following error with default __repr__:
        # AttributeError: 'MemoryObjectItemReceiver' object has no attribute 'item'
        item = getattr(self, "item", None)
        return f"{self.__class__.__name__}(task_info={self.task_info}, item={item!r})"


@dataclass(eq=False)
class _MemoryObjectWaitingSender(Generic[T_Item]):
    item: T_Item
    owner_id: int
    retained: bool = field(init=False, default=False)
    broken: bool = field(init=False, default=False)
    would_block: bool = field(init=False, default=False)


@dataclass(eq=False)
class _MemoryObjectStreamState(Generic[T_Item]):
    max_buffer_size: float = field()
    buffer: deque[T_Item] = field(init=False, default_factory=deque)
    open_send_channels: int = field(init=False, default=0)
    open_receive_channels: int = field(init=False, default=0)
    waiting_receivers: OrderedDict[Event, _MemoryObjectItemReceiver[T_Item]] = field(
        init=False, default_factory=OrderedDict
    )
    waiting_senders: OrderedDict[Event, _MemoryObjectWaitingSender[T_Item]] = field(
        init=False, default_factory=OrderedDict
    )

    def statistics(self) -> MemoryObjectStreamStatistics:
        return MemoryObjectStreamStatistics(
            len(self.buffer),
            self.max_buffer_size,
            self.open_send_channels,
            self.open_receive_channels,
            sum(not sender.retained for sender in self.waiting_senders.values()),
            len(self.waiting_receivers),
        )


def _steady_capacity_available(
    state: _MemoryObjectStreamState[T_Item],
    policy: MemoryObjectStreamPolicy,
) -> bool:
    if not policy.buffer_ledger.steady_cap:
        raise RuntimeError("memory stream policies cannot increase the steady capacity")

    return len(state.buffer) < state.max_buffer_size


def _policy_for_running_owner(owner_id: int) -> MemoryObjectStreamPolicy:
    if not _existing_root_policy_states():
        return _DEFAULT_POLICY

    return _get_effective_policy(_running_task_tree_anchor(owner_id))


def _pin_compatible_shutdown_policy(
    policy: MemoryObjectStreamPolicy,
) -> ChannelRefcountShutdownPolicy:
    shutdown_policy = policy.channel_refcount_shutdown
    if not (
        shutdown_policy.last_receiver_breaks_senders
        and shutdown_policy.last_sender_ends_receivers
        and shutdown_policy.clone_partial_keep_blocked
    ):
        raise RuntimeError("unsupported memory stream shutdown policy")

    return shutdown_policy


def _deliver_to_waiting_receiver(
    state: _MemoryObjectStreamState[T_Item],
    item: T_Item,
) -> bool:
    while state.waiting_receivers:
        receive_event, receiver = state.waiting_receivers.popitem(last=False)
        if not receiver.task_info.has_pending_cancellation():
            receiver.item = item
            receive_event.set()
            return True

    return False


def _reclaim_waiting_sender(
    state: _MemoryObjectStreamState[T_Item],
    send_event: Event,
    sender: _MemoryObjectWaitingSender[T_Item],
    policy: MemoryObjectStreamPolicy,
) -> None:
    if sender.broken or state.open_receive_channels == 0:
        # The item cannot be retained once the receiving side is gone.
        state.waiting_senders.pop(send_event, None)
        return

    if _deliver_to_waiting_receiver(state, sender.item):
        state.waiting_senders.pop(send_event, None)
        return

    if _steady_capacity_available(state, policy):
        state.waiting_senders.pop(send_event, None)
        state.buffer.append(sender.item)
        return

    sender.retained = True


@dataclass(eq=False)
class MemoryObjectReceiveStream(ObjectReceiveStream[T_co], Generic[T_co]):
    _state: _MemoryObjectStreamState[T_co]
    _closed: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self._state.open_receive_channels += 1

    def _receive_nowait(
        self,
        policy: MemoryObjectStreamPolicy,
    ) -> T_co:
        if self._closed:
            raise ClosedResourceError

        item_available = False
        received_item: T_co | object = None
        while self._state.waiting_senders:
            # Get the item from the next sender
            send_event, sender = self._state.waiting_senders.popitem(last=False)
            if not sender.retained:
                sender_policy = _policy_for_running_owner(sender.owner_id)
                if sender_policy.blocked_send_surface.fail_fast:
                    sender.would_block = True
                    send_event.set()
                    continue

            if policy.buffer_ledger.forbid_staging_overshoot:
                if self._state.buffer:
                    received_item = self._state.buffer.popleft()
                    self._state.buffer.append(sender.item)
                else:
                    received_item = sender.item

                send_event.set()
            else:
                self._state.buffer.append(sender.item)
                send_event.set()
                received_item = self._state.buffer.popleft()

            item_available = True
            break

        if item_available:
            return received_item  # type: ignore[return-value]
        elif self._state.buffer:
            return self._state.buffer.popleft()
        elif not self._state.open_send_channels:
            if policy.channel_refcount_shutdown.last_sender_ends_receivers:
                raise EndOfStream

            raise WouldBlock

        raise WouldBlock

    def receive_nowait(self) -> T_co:
        """
        Receive the next item if it can be done without waiting.

        :return: the received item
        :raises ~anyio.ClosedResourceError: if this send stream has been closed
        :raises ~anyio.EndOfStream: if the buffer is empty and this stream has been
            closed from the sending end
        :raises ~anyio.WouldBlock: if there are no items in the buffer and no tasks
            waiting to send

        """
        return self._receive_nowait(_get_effective_policy())

    async def receive(self) -> T_co:
        await checkpoint()
        policy = _get_effective_policy()
        try:
            return self._receive_nowait(policy)
        except WouldBlock:
            # Add ourselves in the queue
            receive_event = Event()
            receiver = _MemoryObjectItemReceiver[T_co]()
            self._state.waiting_receivers[receive_event] = receiver

            try:
                await receive_event.wait()
            finally:
                self._state.waiting_receivers.pop(receive_event, None)

            if receiver.end_of_stream:
                raise EndOfStream from None

            try:
                return receiver.item
            except AttributeError:
                if _get_effective_policy().channel_refcount_shutdown.last_sender_ends_receivers:
                    raise EndOfStream from None

                raise

    def clone(self) -> MemoryObjectReceiveStream[T_co]:
        """
        Create a clone of this receive stream.

        Each clone can be closed separately. Only when all clones have been closed will
        the receiving end of the memory stream be considered closed by the sending ends.

        :return: the cloned stream

        """
        if self._closed:
            raise ClosedResourceError

        _pin_compatible_shutdown_policy(_get_effective_policy())
        return MemoryObjectReceiveStream(_state=self._state)

    def close(self) -> None:
        """
        Close the stream.

        This works the exact same way as :meth:`aclose`, but is provided as a special
        case for the benefit of synchronous callbacks.

        """
        if not self._closed:
            policy = _get_effective_policy()
            self._closed = True
            self._state.open_receive_channels -= 1
            shutdown_policy = _pin_compatible_shutdown_policy(policy)
            if (
                self._state.open_receive_channels > 0
                and shutdown_policy.clone_partial_keep_blocked
            ):
                return

            if shutdown_policy.last_receiver_breaks_senders:
                waiting_senders = list(self._state.waiting_senders.items())
                for send_event, sender in waiting_senders:
                    if sender.retained:
                        self._state.waiting_senders.pop(send_event, None)
                    else:
                        sender.broken = True
                        send_event.set()

    async def aclose(self) -> None:
        self.close()

    def statistics(self) -> MemoryObjectStreamStatistics:
        """
        Return statistics about the current state of this stream.

        .. versionadded:: 3.0
        """
        return self._state.statistics()

    def __enter__(self) -> MemoryObjectReceiveStream[T_co]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        if not self._closed:
            warnings.warn(
                f"Unclosed <{self.__class__.__name__} at {id(self):x}>",
                ResourceWarning,
                stacklevel=1,
                source=self,
            )


@dataclass(eq=False)
class MemoryObjectSendStream(ObjectSendStream[T_contra], Generic[T_contra]):
    _state: _MemoryObjectStreamState[T_contra]
    _closed: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self._state.open_send_channels += 1

    def _send_nowait(
        self,
        item: T_contra,
        policy: MemoryObjectStreamPolicy,
    ) -> None:
        if self._closed:
            raise ClosedResourceError
        if not self._state.open_receive_channels:
            raise BrokenResourceError

        if _deliver_to_waiting_receiver(self._state, item):
            return

        if not _steady_capacity_available(self._state, policy):
            surface = policy.blocked_send_surface
            if surface.queue_waiters or surface.fail_fast:
                raise WouldBlock

        self._state.buffer.append(item)

    def send_nowait(self, item: T_contra) -> None:
        """
        Send an item immediately if it can be done without waiting.

        :param item: the item to send
        :raises ~anyio.ClosedResourceError: if this send stream has been closed
        :raises ~anyio.BrokenResourceError: if the stream has been closed from the
            receiving end
        :raises ~anyio.WouldBlock: if the buffer is full and there are no tasks waiting
            to receive

        """
        self._send_nowait(item, _get_effective_policy())

    async def send(self, item: T_contra) -> None:
        """
        Send an item to the stream.

        If the buffer is full, this method blocks until there is again room in the
        buffer or the item can be sent directly to a receiver.

        :param item: the item to send
        :raises ~anyio.ClosedResourceError: if this send stream has been closed
        :raises ~anyio.BrokenResourceError: if the stream has been closed from the
            receiving end

        """
        await checkpoint()
        policy = _get_effective_policy()
        try:
            self._send_nowait(item, policy)
        except WouldBlock:
            surface = policy.blocked_send_surface
            if surface.fail_fast or not surface.queue_waiters:
                raise

            # Wait until there's someone on the receiving end
            send_event = Event()
            current_task = get_current_task()
            sender = _MemoryObjectWaitingSender[T_contra](item, current_task.id)
            self._state.waiting_senders[send_event] = sender
            try:
                await send_event.wait()
            except BaseException:
                queued_sender = self._state.waiting_senders.get(send_event)
                if queued_sender is not None:
                    sender_policy = _policy_for_running_owner(queued_sender.owner_id)
                    if sender_policy.blocked_send_surface.reclaim_on_send_cancel:
                        _reclaim_waiting_sender(
                            self._state,
                            send_event,
                            queued_sender,
                            sender_policy,
                        )
                    else:
                        self._state.waiting_senders.pop(send_event, None)

                raise

            if sender.broken:
                self._state.waiting_senders.pop(send_event, None)
                raise BrokenResourceError from None
            if sender.would_block:
                self._state.waiting_senders.pop(send_event, None)
                raise WouldBlock from None
            if send_event in self._state.waiting_senders:
                del self._state.waiting_senders[send_event]
                raise BrokenResourceError from None

    def clone(self) -> MemoryObjectSendStream[T_contra]:
        """
        Create a clone of this send stream.

        Each clone can be closed separately. Only when all clones have been closed will
        the sending end of the stream be considered closed by the receiving ends.

        :return: the cloned stream

        """
        if self._closed:
            raise ClosedResourceError

        _pin_compatible_shutdown_policy(_get_effective_policy())
        return MemoryObjectSendStream(_state=self._state)

    def close(self) -> None:
        """
        Close the stream.

        This works the exact same way as :meth:`aclose`, but is provided as a special
        case for the benefit of synchronous callbacks.

        """
        if not self._closed:
            policy = _get_effective_policy()
            self._closed = True
            self._state.open_send_channels -= 1
            shutdown_policy = _pin_compatible_shutdown_policy(policy)
            if (
                self._state.open_send_channels > 0
                and shutdown_policy.clone_partial_keep_blocked
            ):
                return

            if shutdown_policy.last_sender_ends_receivers:
                for receiver in self._state.waiting_receivers.values():
                    receiver.end_of_stream = True

                receive_events = list(self._state.waiting_receivers.keys())
                self._state.waiting_receivers.clear()
                for event in receive_events:
                    event.set()

    async def aclose(self) -> None:
        self.close()

    def statistics(self) -> MemoryObjectStreamStatistics:
        """
        Return statistics about the current state of this stream.

        .. versionadded:: 3.0
        """
        return self._state.statistics()

    def __enter__(self) -> MemoryObjectSendStream[T_contra]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        if not self._closed:
            warnings.warn(
                f"Unclosed <{self.__class__.__name__} at {id(self):x}>",
                ResourceWarning,
                stacklevel=1,
                source=self,
            )
