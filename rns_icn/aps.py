"""APSManager — subscription tracking and push delivery for APS Subscribe.

APS (Asynchronous Publish-Subscribe) upgrades a link to push mode.
Once a consumer subscribes to a stream name, the producer pushes
Data packets as they're produced without requiring per-packet Interests.

Modeled on LXMF's handle_outbound pattern — once a link is established,
messages are pushed freely. The subscribe Interest is a one-time handshake
after which Data flows automatically.

Usage:
    manager = APSManager(server)
    manager.subscribe(name, face_id)   # register subscription
    manager.unsubscribe(name, face_id) # remove single subscription
    await manager.publish(data)         # push to all subscribers
"""

from __future__ import annotations

from typing import Optional

from .face import FaceId
from .name import Name
from .packet import Data


class APSManager:
    """Tracks subscriptions and delivers pushed Data to subscribers.

    Manages a mapping of stream names to sets of FaceIds.
    When Data is published via publish(), it's pushed to all
    faces subscribed to that stream (or any prefix of it).
    """

    def __init__(self, server: "ICNServer" = None):  # noqa: F821
        self._server = server
        self._subscriptions: dict[Name, set[FaceId]] = {}

    def subscribe(self, name: Name, face_id: FaceId) -> None:
        """Register a face as subscriber to a stream name prefix."""
        if name not in self._subscriptions:
            self._subscriptions[name] = set()
        self._subscriptions[name].add(face_id)

    def unsubscribe(self, name: Name, face_id: FaceId) -> None:
        """Remove a face from a specific subscription."""
        subs = self._subscriptions.get(name)
        if subs:
            subs.discard(face_id)
            if not subs:
                del self._subscriptions[name]

    def unsubscribe_face(self, face_id: FaceId) -> None:
        """Remove all subscriptions for a given face (on link teardown)."""
        for name in list(self._subscriptions.keys()):
            self._subscriptions[name].discard(face_id)
            if not self._subscriptions[name]:
                del self._subscriptions[name]

    def get_subscriber_faces(self, name: Name) -> list[FaceId]:
        """Get all faces subscribed to a stream name, using prefix matching.

        A subscription to '/alice/stream' matches published data for
        '/alice/stream/seg5' (longer name, subscriber prefix match).
        Also matches '/alice/stream' itself (exact match).
        """
        result: set[FaceId] = set()
        for stream_name, faces in self._subscriptions.items():
            # A subscribed name is a prefix of the published data name,
            # OR the published data name is a prefix of the subscribed name
            if name.starts_with(stream_name) or stream_name.starts_with(name):
                result.update(faces)
        return list(result)

    def is_subscribed(self, name: Name, face_id: FaceId) -> bool:
        """Check if a face is subscribed to a stream name."""
        for stream_name, faces in self._subscriptions.items():
            if face_id in faces and (
                name.starts_with(stream_name) or stream_name.starts_with(name)
            ):
                return True
        return False

    def subscription_count(self) -> int:
        """Total number of subscription entries across all streams."""
        return sum(len(faces) for faces in self._subscriptions.values())

    def stream_count(self) -> int:
        """Number of distinct stream names with subscribers."""
        return len(self._subscriptions)

    async def publish(
        self,
        data: Data,
        *,
        offline_queue=None,
    ) -> int:
        """Push a Data packet to all subscribers of its stream.

        Sends the Data to every face subscribed to a stream name
        that matches the Data's name (prefix-matched).
        If no server reference is set, this is a no-op.

        Args:
            data: Data packet to push.
            offline_queue: Optional OfflineQueue — unreachable subscribers'
                data is enqueued here instead of dropped. The data is
                keyed under the subscriber's stream name so that
                drain(stream_name) can retrieve it on reconnect.

        Returns:
            Number of subscribers the data was successfully sent to.
        """
        if self._server is None:
            return 0
        subscribers = self.get_subscriber_faces(data.name)
        sent = 0
        for face_id in subscribers:
            face = self._server._faces.get(face_id)
            if face:
                try:
                    await face.send_data(data)
                    sent += 1
                except Exception:
                    if offline_queue is not None:
                        # Enqueue under the stream names the face subscribed to
                        self._enqueue_for_face(face_id, data, offline_queue)
            elif offline_queue is not None:
                # Face is gone — queue under the subscriber's stream names
                self._enqueue_for_face(face_id, data, offline_queue)
        return sent

    def _enqueue_for_face(self, face_id: FaceId, data: Data,
                          offline_queue) -> None:
        """Enqueue data under each stream name the face subscribed to."""
        for stream_name in self._subscriptions:
            faces = self._subscriptions[stream_name]
            if face_id in faces and (
                data.name.starts_with(stream_name)
                or stream_name.starts_with(data.name)
            ):
                offline_queue.put(stream_name, data)
