"""Pub/Sub-based client for communicating with workers.

Replaces the gRPC-based communication with pub/sub topics.
"""

import json
import logging
import time
import uuid
from typing import Dict, Any, Optional
from google.cloud import pubsub_v1
from google.protobuf import duration_pb2
from concurrent.futures import TimeoutError as FuturesTimeoutError

log = logging.getLogger(__name__)

# Subscription expiration: auto-delete after 1 hour of inactivity
SUBSCRIPTION_EXPIRATION_SECONDS = 3600

# Default timeout for waiting for a response
DEFAULT_TIMEOUT = 20.0


class PubSubMonitorClient:
    """Client for communicating with workers via pub/sub.

    Publishes requests to the incoming topic and subscribes to the
    response topic to receive replies.
    """

    def __init__(
        self,
        project_id: str,
        incoming_topic: str,
        response_topic: str,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        self.project_id = project_id
        self.incoming_topic = incoming_topic
        self.response_topic = response_topic
        self.timeout = timeout

        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()

        self.incoming_topic_path = self.publisher.topic_path(project_id, incoming_topic)
        self.response_topic_path = self.publisher.topic_path(project_id, response_topic)

        # Create a unique subscription for this client instance
        self.subscription_id = f"sparkles-cli-{uuid.uuid4().hex[:8]}"
        self.subscription_path = self.subscriber.subscription_path(
            project_id, self.subscription_id
        )

        # Pending responses keyed by request_id
        self._pending_responses: Dict[str, Any] = {}
        self._streaming_pull_future = None

        self._setup_subscription()

    def _setup_subscription(self):
        """Create a temporary subscription for receiving responses."""
        try:
            # Set expiration policy so subscription auto-deletes after inactivity
            expiration_policy = pubsub_v1.types.ExpirationPolicy(
                ttl=duration_pb2.Duration(seconds=SUBSCRIPTION_EXPIRATION_SECONDS)
            )
            self.subscriber.create_subscription(
                name=self.subscription_path,
                topic=self.response_topic_path,
                ack_deadline_seconds=30,
                expiration_policy=expiration_policy,
            )
            log.debug(f"Created subscription: {self.subscription_path}")
        except Exception as e:
            # Subscription might already exist
            log.debug(f"Subscription setup: {e}")

        # Start receiving messages
        def callback(message):
            try:
                data = json.loads(message.data.decode("utf-8"))
                request_id = data.get("request_id")
                if request_id and request_id in self._pending_responses:
                    self._pending_responses[request_id] = data
                message.ack()
            except Exception as e:
                log.warning(f"Error processing response message: {e}")
                message.nack()

        self._streaming_pull_future = self.subscriber.subscribe(
            self.subscription_path, callback=callback
        )

    def _send_request(
        self, message_type: str, payload: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Send a request and wait for the response."""
        request_id = uuid.uuid4().hex

        request = {
            "type": message_type,
            "request_id": request_id,
            "payload": payload or {},
        }

        # Register that we're waiting for this response
        self._pending_responses[request_id] = None

        # Publish the request
        data = json.dumps(request).encode("utf-8")
        future = self.publisher.publish(self.incoming_topic_path, data)
        future.result(timeout=10)  # Wait for publish to complete

        # Wait for the response
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            response = self._pending_responses.get(request_id)
            if response is not None:
                del self._pending_responses[request_id]
                return response
            time.sleep(0.1)

        # Timeout
        del self._pending_responses[request_id]
        raise TimeoutError(f"Timeout waiting for response to {message_type}")

    def read_output(self, task_id: str, offset: int, size: int) -> Dict[str, Any]:
        """Read output from a task.

        Returns a dict with keys: success, data, end_of_file (or error on failure)
        """
        try:
            response = self._send_request(
                "read_output",
                {"task_id": task_id, "offset": offset, "size": size},
            )

            if response.get("error"):
                return {"success": False, "error": response["error"]}

            payload = response.get("payload", {})
            # Data is base64 encoded in JSON, decode it
            import base64

            data = base64.b64decode(payload.get("data", ""))
            return {
                "success": True,
                "data": data,
                "end_of_file": payload.get("end_of_file", False),
            }
        except TimeoutError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_process_status(self) -> Dict[str, Any]:
        """Get process status from the worker.

        Returns a dict with keys: success, process_count, total_memory, etc.
        """
        try:
            response = self._send_request("get_process_status")

            if response.get("error"):
                return {"success": False, "error": response["error"]}

            payload = response.get("payload", {})
            return {
                "success": True,
                "process_count": payload.get("process_count", 0),
                "total_memory": payload.get("total_memory", 0),
                "total_data": payload.get("total_data", 0),
                "total_shared": payload.get("total_shared", 0),
                "total_resident": payload.get("total_resident", 0),
            }
        except TimeoutError as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def close(self):
        """Clean up resources."""
        # Cancel the streaming pull
        if self._streaming_pull_future:
            self._streaming_pull_future.cancel()
            try:
                self._streaming_pull_future.result(timeout=5)
            except (FuturesTimeoutError, Exception):
                pass

        # Delete the temporary subscription
        try:
            self.subscriber.delete_subscription(subscription=self.subscription_path)
            log.debug(f"Deleted subscription: {self.subscription_path}")
        except Exception as e:
            log.debug(f"Error deleting subscription: {e}")
