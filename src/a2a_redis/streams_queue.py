"""Redis Streams-backed event queue implementation for the A2A Python SDK.

This module provides a Redis Streams-based implementation of EventQueue,
offering persistent, reliable event delivery with consumer groups, acknowledgments, and replay capability.

**Key Features**:
- **Persistent storage**: Events remain in streams until explicitly trimmed
- **Guaranteed delivery**: Consumer groups with acknowledgments prevent message loss
- **Load balancing**: Multiple consumers can share work via consumer groups
- **Failure recovery**: Unacknowledged messages can be reclaimed by other consumers
- **Event replay**: Historical events can be re-read from any point in time
- **Ordering**: Maintains strict insertion order with unique message IDs

**Use Cases**:
- Task event queues requiring reliability
- Audit trails and event history
- Work distribution systems
- Systems requiring failure recovery
- Multi-consumer load balancing

**Trade-offs**:
- Higher memory usage (events persist)
- More complex setup (consumer groups)
- Slightly higher latency than pub/sub

For real-time, fire-and-forget scenarios, consider RedisPubSubEventQueue instead.
"""

import asyncio
from typing import Optional, Union

import redis.asyncio as redis
from a2a.types import Message, Task, TaskArtifactUpdateEvent, TaskStatusUpdateEvent

from .event_queue_protocol import EventQueueProtocol
from .streams_consumer_strategy import ConsumerGroupConfig
from .model_utils import (
    serialize_event,
    deserialize_event,
    serialize_to_json,
    deserialize_from_json,
)


class RedisStreamsEventQueue:
    """Redis Streams-backed EventQueue for persistent, reliable event delivery.

    Provides guaranteed delivery with consumer groups, acknowledgments, and replay
    capability. See README.md for detailed use cases and trade-offs.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        task_id: str,
        prefix: str = "stream:",
        consumer_config: Optional[ConsumerGroupConfig] = None,
    ):
        """Initialize Redis Streams event queue.

        Args:
            redis_client: Redis client instance
            task_id: Task identifier this queue is for
            prefix: Key prefix for stream storage
            consumer_config: Consumer group configuration
        """
        self.redis = redis_client
        self.task_id = task_id
        self.prefix = prefix
        # close() semantics:
        # - close(immediate=False): producer shutdown (stop enqueue; consumers may drain)
        # - close(immediate=True): consumer shutdown (stop dequeue; used by EventConsumer on final event)
        self._closing = False
        self._closed = False
        self._stream_key = f"{prefix}{task_id}"

        # Consumer group configuration
        self.consumer_config = consumer_config or ConsumerGroupConfig()
        self.consumer_group = self.consumer_config.get_consumer_group_name(task_id)
        self.consumer_id = self.consumer_config.get_consumer_id()

        # Consumer group will be ensured on first use
        self._consumer_group_ensured = False

    async def _ensure_consumer_group(self) -> None:
        """Create consumer group if it doesn't exist."""
        try:
            # XGROUP CREATE stream_key group_name 0 MKSTREAM
            await self.redis.xgroup_create(
                self._stream_key, self.consumer_group, id="0", mkstream=True
            )  # type: ignore[misc]
        except Exception as e:  # type: ignore[misc]
            if "BUSYGROUP" not in str(e):  # Group already exists
                raise

    async def enqueue_event(
        self,
        event: Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent],
    ) -> None:
        """Add an event to the stream.

        Args:
            event: Event to add to the stream

        Raises:
            RuntimeError: If queue is closed
        """
        if self._closing or self._closed:
            # a2a-sdk closes the queue after agent run; reject new enqueues
            # while allowing consumers to drain already-enqueued events.
            raise RuntimeError("Cannot enqueue to closing/closed queue")

        # Ensure consumer group exists on first use
        if not self._consumer_group_ensured:
            await self._ensure_consumer_group()
            self._consumer_group_ensured = True

        # Serialize event using shared utility
        event_structure = serialize_event(event)

        # Create stream entry with event data
        fields = {
            "event_type": event_structure["event_type"],
            "event_data": serialize_to_json(event_structure["event_data"]),
        }
        # Add to Redis stream (XADD)
        await self.redis.xadd(self._stream_key, fields)  # type: ignore[misc]

    async def dequeue_event(
        self, no_wait: bool = False
    ) -> Union[Message, Task, TaskStatusUpdateEvent, TaskArtifactUpdateEvent]:
        """Remove and return an event from the stream.

        Args:
            no_wait: If True, return immediately if no events available

        Returns:
            Reconstructed Pydantic model (Message, Task, TaskStatusUpdateEvent, or TaskArtifactUpdateEvent)

        Raises:
            asyncio.QueueEmpty: If no events are available or queue is closed.
            asyncio.TimeoutError: If no events within block timeout (no_wait=False).
              Matches a2a-sdk EventConsumer expectations.
        """
        if self._closed:
            raise asyncio.QueueEmpty("Queue is closed")

        # Ensure consumer group exists on first use
        if not self._consumer_group_ensured:
            await self._ensure_consumer_group()
            self._consumer_group_ensured = True

        # Read from consumer group
        timeout = 0 if no_wait else 1000  # 0 = non-blocking, 1000ms block

        try:
            # XREADGROUP GROUP group_name consumer_id COUNT 1 BLOCK timeout STREAMS stream_key >
            result = await self.redis.xreadgroup(
                self.consumer_group,
                self.consumer_id,
                {self._stream_key: ">"},
                count=1,
                block=timeout,
            )  # type: ignore[misc]

            if not result or not result[0][1]:  # No messages available
                # If producer has signaled close and there are no messages, mark closed and stop consumer.
                if self._closing:
                    self._closed = True
                    raise asyncio.QueueEmpty("Queue is closed")
                # No events: raise so EventConsumer can continue polling or exit by contract.
                if no_wait:
                    # EventConsumer.consume_one() expects QueueEmpty when no_wait and no events.
                    raise asyncio.QueueEmpty("No events available")
                raise asyncio.TimeoutError("No events available")

            # Extract message data
            _, messages = result[0]
            message_id, fields = messages[0]

            # Deserialize event data using shared utility
            event_structure = {
                "event_type": fields[b"event_type"].decode()
                if b"event_type" in fields
                else None,
                "event_data": deserialize_from_json(fields[b"event_data"]),
            }

            # Acknowledge the message
            await self.redis.xack(self._stream_key, self.consumer_group, message_id)  # type: ignore[misc]

            # Reconstruct the actual Pydantic model using shared utility
            return deserialize_event(event_structure)

        except (asyncio.TimeoutError, TimeoutError, asyncio.QueueEmpty):
            # Re-raise so EventConsumer handles them as control-flow exceptions.
            raise
        except Exception as e:  # type: ignore[misc]
            if "NOGROUP" in str(e):
                # Consumer group was deleted, recreate it
                await self._ensure_consumer_group()
                if no_wait:
                    raise asyncio.QueueEmpty(
                        "Consumer group recreated, try again"
                    ) from e
                raise asyncio.TimeoutError("Consumer group recreated, try again") from e
            raise RuntimeError(f"Error reading from stream: {e}") from e

    async def close(self, immediate: bool = False) -> None:
        """Close the queue.

        Args:
            immediate:
              - False (default): Stop enqueue; set _closing so consumers can drain.
              - True: Stop dequeue (used by a2a-sdk EventConsumer on final event).
                Set _closed and run pending cleanup (xpending_range + xack for this consumer).
        """
        if immediate:
            self._closed = True
            await self._close_pending_cleanup()
            return

        self._closing = True

    async def _close_pending_cleanup(self) -> None:
        """Ack this consumer's pending messages (xpending_range + xack) for cleanup."""
        try:
            pending = await self.redis.xpending_range(
                self._stream_key,
                self.consumer_group,
                min="-",
                max="+",
                count=100,
                consumername=self.consumer_id,
            )  # type: ignore[misc]
            if pending:
                message_ids = [msg["message_id"] for msg in pending]
                await self.redis.xack(
                    self._stream_key, self.consumer_group, *message_ids
                )  # type: ignore[misc]
        except Exception:  # type: ignore[misc]
            # Ignore e.g. NOGROUP (consumer group missing) so close() completes without raising.
            pass

    def is_closed(self) -> bool:
        """Check if the queue is closed."""
        return self._closed

    def tap(self) -> "EventQueueProtocol":
        """Create a tap (copy) of this queue.

        Creates a new queue with the same stream but different consumer ID
        for independent message processing.
        """
        return RedisStreamsEventQueue(
            self.redis, self.task_id, self.prefix, self.consumer_config
        )

    def task_done(self) -> None:
        """Mark a task as done (for compatibility)."""
        pass  # Stream acknowledgment is handled in dequeue_event
