#  Copyright 2020 Unity Technologies
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import re
import threading

from rclpy.action import ActionServer, CancelResponse, GoalResponse
from rclpy.callback_groups import ReentrantCallbackGroup

from .communication import RosReceiver


class ActionGoalBridge:
    """
    One in-flight goal: the queue the client thread drops feedback into, and the
    event that says the result has arrived.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.feedback = []
        self.updated = threading.Event()
        self.result = None
        self.status = 0
        self.done = False

    def push_feedback(self, message):
        with self.lock:
            self.feedback.append(message)
        self.updated.set()

    def finish(self, result, status):
        with self.lock:
            self.result = result
            self.status = status
            self.done = True
        self.updated.set()

    def drain(self):
        """Take everything that has arrived since the last call."""
        with self.lock:
            feedback = self.feedback
            self.feedback = []
            return feedback, self.done


class UnityAction(RosReceiver):
    """
    Registers a ROS action server whose goals are executed in Unity.

    The Unity side never rejects a goal: doing so would need another round trip
    before execution begins, and interfaces that can refuse a goal say so in the
    result instead. So every goal is accepted and the outcome comes back as the
    status on the result.
    """

    def __init__(self, topic, action_class, tcp_server, queue_size=10):
        strippedTopic = re.sub("[^A-Za-z0-9_]+", "", topic)
        node_name = f"{strippedTopic}_action"
        RosReceiver.__init__(self, node_name)

        self.topic = topic
        self.node_name = node_name
        self.action_class = action_class
        self.tcp_server = tcp_server
        self.queue_size = queue_size

        # A reentrant group so a cancel request can be served while execute_callback
        # is still blocked waiting on Unity.
        self.action_server = ActionServer(
            self,
            action_class,
            topic,
            execute_callback=self.execute,
            goal_callback=self.on_goal,
            cancel_callback=self.on_cancel,
            callback_group=ReentrantCallbackGroup(),
        )

    def on_goal(self, goal_request):
        return GoalResponse.ACCEPT

    def on_cancel(self, goal_handle):
        return CancelResponse.ACCEPT

    def execute(self, goal_handle):
        """
        Hand the goal to Unity, relay feedback, and return the result.

        Runs on an executor thread, so blocking here is fine as long as the
        executor has threads to spare (TcpServer.setup_executor counts actions).
        """
        action_id, bridge = self.tcp_server.unity_tcp_sender.send_unity_action_goal(
            self.topic, goal_handle.request
        )
        if bridge is None:
            self.tcp_server.logerr(
                "No Unity connection to run action '{}'".format(self.topic)
            )
            goal_handle.abort()
            return self.action_class.Result()

        cancel_sent = False
        try:
            while True:
                bridge.updated.wait(timeout=0.1)
                bridge.updated.clear()

                if goal_handle.is_cancel_requested and not cancel_sent:
                    # Tell Unity once; it decides when to stop and still owns the
                    # result, so we keep waiting for it either way.
                    self.tcp_server.unity_tcp_sender.send_unity_action_cancel(
                        self.topic, action_id
                    )
                    cancel_sent = True

                feedback_messages, done = bridge.drain()
                for feedback in feedback_messages:
                    goal_handle.publish_feedback(feedback)

                if done:
                    break
        finally:
            self.tcp_server.unity_tcp_sender.forget_action(action_id)

        # GoalStatus values, matching ActionGoalStatus.cs on the Unity side.
        if bridge.status == 5:
            goal_handle.canceled()
        elif bridge.status == 6:
            goal_handle.abort()
        else:
            goal_handle.succeed()

        return bridge.result if bridge.result is not None else self.action_class.Result()

    def unregister(self):
        try:
            self.action_server.destroy()
        except Exception:  # noqa: BLE001 - shutting down anyway
            pass
        self.destroy_node()
