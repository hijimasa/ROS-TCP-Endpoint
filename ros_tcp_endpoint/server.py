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

import rclpy
import socket
import json
import sys
import threading
import importlib

from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.executors import MultiThreadedExecutor
from rclpy.exceptions import InvalidHandle
from rclpy.serialization import deserialize_message

from .tcp_sender import UnityTcpSender
from .client import ClientThread
from .subscriber import RosSubscriber
from .publisher import RosPublisher
from .service import RosService
from .unity_service import UnityService
from .unity_action import UnityAction


class TcpServer(Node):
    """
    Initializes ROS node and TCP server.
    """

    # Lower bound on executor threads. One goal in flight needs a thread for its
    # execute_callback and another for the cancel request that may arrive while it
    # runs; the rest is headroom for services and topics registering later.
    MIN_EXECUTOR_THREADS = 8

    def __init__(self, node_name, buffer_size=1024, connections=10, tcp_ip=None, tcp_port=None):
        """
        Initializes ROS node and class variables.

        Args:
            node_name:               ROS node name for executing code
            buffer_size:             The read buffer size used when reading from a socket
            connections:             Max number of queued connections. See Python Socket documentation
        """
        super().__init__(node_name)

        self.declare_parameter("ROS_IP", "0.0.0.0")
        self.declare_parameter("ROS_TCP_PORT", 10000)

        if tcp_ip:
            self.loginfo("Using ROS_IP override from constructor: {}".format(tcp_ip))
            self.tcp_ip = tcp_ip
        else:
            self.tcp_ip = self.get_parameter("ROS_IP").get_parameter_value().string_value

        if tcp_port:
            self.loginfo("Using ROS_TCP_PORT override from constructor: {}".format(tcp_port))
            self.tcp_port = tcp_port
        else:
            self.tcp_port = self.get_parameter("ROS_TCP_PORT").get_parameter_value().integer_value

        self.unity_tcp_sender = UnityTcpSender(self)

        self.node_name = node_name
        self.publishers_table = {}
        self.subscribers_table = {}
        self.ros_services_table = {}
        self.unity_services_table = {}
        self.unity_actions_table = {}
        self.buffer_size = buffer_size
        self.connections = connections
        self.syscommands = SysCommands(self)
        self.pending_srv_id = None
        self.pending_srv_is_request = False
        # Set when Unity announces that the next message is action feedback or a
        # result, mirroring pending_srv_id for services.
        self.pending_action_id = None
        self.pending_action_is_result = False
        self.pending_action_status = 0
        # start() spawns the listen thread before setup_executor() runs, so a
        # client can connect and register a topic while this is still unset.
        # Without the default, that raced into an AttributeError which killed
        # the client thread (and with it every topic and service).
        self.executor = None

    def start(self, publishers=None, subscribers=None):
        if publishers is not None:
            self.publishers_table = publishers
        if subscribers is not None:
            self.subscribers_table = subscribers
        server_thread = threading.Thread(target=self.listen_loop)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()

    def listen_loop(self):
        """
            Creates and binds sockets using TCP variables then listens for incoming connections.
            For each new connection a client thread will be created to handle communication.
        """
        self.loginfo("Starting server on {}:{}".format(self.tcp_ip, self.tcp_port))
        tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_server.bind((self.tcp_ip, self.tcp_port))

        while True:
            tcp_server.listen(self.connections)

            try:
                (conn, (ip, port)) = tcp_server.accept()
                ClientThread(conn, self, ip, port).start()
            except socket.timeout as err:
                self.logerr("ros_tcp_endpoint.TcpServer: socket timeout")

    def send_unity_error(self, error):
        self.unity_tcp_sender.send_unity_error(error)

    def send_unity_message(self, topic, message):
        self.unity_tcp_sender.send_unity_message(topic, message)

    def send_unity_service(self, topic, service_class, request):
        return self.unity_tcp_sender.send_unity_service_request(topic, service_class, request)

    def send_unity_service_response(self, srv_id, data):
        self.unity_tcp_sender.send_unity_service_response(srv_id, data)

    def handle_syscommand(self, topic, data):
        """
        Dispatch a "__..." system command coming from the Unity side.

        Nothing in here may raise. ClientThread.run() only catches IOError, so
        any other exception escaping this call tears down the client thread in
        its finally block; from Unity's point of view every topic and every
        service goes silent at once. An unknown command or a malformed payload
        must therefore be reported, not propagated.
        """
        command = topic[2:]

        if command not in SysCommands.COMMANDS:
            self.logerr("Unknown SysCommand '{}'".format(topic))
            self.send_unity_error("Don't understand SysCommand.'{}'".format(topic))
            return

        function = getattr(self.syscommands, command, None)
        if not callable(function):
            self.logerr("SysCommand '{}' is declared but not implemented".format(topic))
            self.send_unity_error("SysCommand.'{}' is not implemented".format(topic))
            return

        try:
            message_json = data.decode("utf-8")[:-1]
            params = json.loads(message_json)
            function(**params)
        except Exception as e:
            # Includes UnicodeDecodeError / JSONDecodeError for a corrupt
            # payload and TypeError for arguments the command does not accept.
            self.logerr("SysCommand '{}' failed: {}: {}".format(topic, type(e).__name__, e))
            self.send_unity_error("SysCommand.'{}' failed: {}".format(topic, e))

    def loginfo(self, text):
        self.get_logger().info(text)

    def logwarn(self, text):
        self.get_logger().warning(text)

    def logerr(self, text):
        self.get_logger().error(text)

    def setup_executor(self):
        """
            Since rclpy.spin() is a blocking call the server needed a way
            to spin all of the relevant nodes at the same time.

            MultiThreadedExecutor allows us to set the number of threads
            needed as well as the nodes that need to be spun.
        """
        # Almost everything registers *after* this runs, because the tables are
        # filled by sys commands from Unity once it connects. Sizing the pool from
        # the tables alone therefore yields 1 thread in practice, which deadlocks
        # actions: execute_callback holds its thread for the whole goal, and the
        # cancel request for that same goal then has no thread left to run on.
        # Hence a floor that does not depend on the tables.
        num_threads = max(
            len(self.publishers_table.keys())
            + len(self.subscribers_table.keys())
            + len(self.ros_services_table.keys())
            + len(self.unity_services_table.keys())
            + len(self.unity_actions_table.keys()) * 2
            + 1,
            self.MIN_EXECUTOR_THREADS,
        )
        executor = MultiThreadedExecutor(num_threads)

        executor.add_node(self)

        for ros_node in self.publishers_table.values():
            executor.add_node(ros_node)
        for ros_node in self.subscribers_table.values():
            executor.add_node(ros_node)
        for ros_node in self.ros_services_table.values():
            executor.add_node(ros_node)
        for ros_node in self.unity_services_table.values():
            executor.add_node(ros_node)
        for ros_node in self.unity_actions_table.values():
            executor.add_node(ros_node)

        self.executor = executor
        while rclpy.ok():
            try:
                executor.spin()
                break
            except InvalidHandle as e:
                # A subscriber/publisher/service node was destroyed (e.g. on
                # Unity re-registration) while the executor was reading from
                # its handle. rclpy Humble does not catch this in
                # _take_subscription, so spin() exits. Resume spinning so
                # message transport does not silently stop.
                self.logwarn(
                    "Executor caught InvalidHandle during spin "
                    "(likely a node re-registration race); resuming: {}".format(e)
                )

    def unregister_node(self, old_node):
        if old_node is not None:
            if self.executor is not None:
                self.executor.remove_node(old_node)
            old_node.unregister()

    def destroy_nodes(self):
        """
            Clean up all of the nodes
        """
        for ros_node in self.publishers_table.values():
            ros_node.destroy_node()
        for ros_node in self.subscribers_table.values():
            ros_node.destroy_node()
        for ros_node in self.ros_services_table.values():
            ros_node.destroy_node()
        for ros_node in self.unity_services_table.values():
            ros_node.destroy_node()
        for ros_node in self.unity_actions_table.values():
            ros_node.destroy_node()

        self.destroy_node()


class SysCommands:
    # The commands the Unity side may invoke, matching SysCommand.cs in
    # ROS-TCP-Connector (minus the "__" prefix). Dispatch is driven by data off
    # the socket, so it goes through this allow list rather than a bare getattr
    # over every attribute of this class.
    COMMANDS = frozenset(
        {
            "subscribe",
            "publish",
            "ros_service",
            "unity_service",
            "remove_subscriber",
            "remove_publisher",
            "remove_ros_service",
            "remove_unity_service",
            "unity_action",
            "remove_unity_action",
            "action_feedback",
            "action_result",
            "response",
            "request",
            "topic_list",
        }
    )

    def __init__(self, tcp_server):
        self.tcp_server = tcp_server

    def subscribe(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't subscribe to a blank topic name! SysCommand.subscribe({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.subscribe - Unknown message class '{}'".format(message_name)
            )
            return

        old_node = self.tcp_server.subscribers_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_subscriber = RosSubscriber(topic, message_class, self.tcp_server)
        self.tcp_server.subscribers_table[topic] = new_subscriber
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_subscriber)

        self.tcp_server.loginfo("RegisterSubscriber({}, {}) OK".format(topic, message_class))

    def publish(self, topic, message_name, queue_size=10, latch=False):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't publish to a blank topic name! SysCommand.publish({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.publish - Unknown message class '{}'".format(message_name)
            )
            return

        old_node = self.tcp_server.publishers_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_publisher = RosPublisher(topic, message_class, queue_size=queue_size, latch=latch)

        self.tcp_server.publishers_table[topic] = new_publisher
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_publisher)

        self.tcp_server.loginfo("RegisterPublisher({}, {}) OK".format(topic, message_class))

    def ros_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return
        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        old_node = self.tcp_server.ros_services_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_service = RosService(topic, message_class)

        self.tcp_server.ros_services_table[topic] = new_service
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_service)

        self.tcp_server.loginfo("RegisterRosService({}, {}) OK".format(topic, message_class))

    def unity_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        old_node = self.tcp_server.unity_services_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_service = UnityService(str(topic), message_class, self.tcp_server)

        self.tcp_server.unity_services_table[topic] = new_service
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_service)

        self.tcp_server.loginfo("RegisterUnityService({}, {}) OK".format(topic, message_class))

    def unity_action(self, topic, message_name):
        """Register a ROS action server whose goals are executed in Unity."""
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterUnityAction({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return

        action_class = self.resolve_message_name(message_name, "action")
        if action_class is None:
            self.tcp_server.send_unity_error(
                "RegisterUnityAction({}, {}) - Unknown action class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        old_node = self.tcp_server.unity_actions_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_action = UnityAction(str(topic), action_class, self.tcp_server)

        self.tcp_server.unity_actions_table[topic] = new_action
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_action)

        self.tcp_server.loginfo("RegisterUnityAction({}, {}) OK".format(topic, action_class))

    def remove_unity_action(self, topic):
        self._remove(self.tcp_server.unity_actions_table, topic, "UnityAction")

    def action_feedback(self, action_id):
        # the next message is a feedback message for this goal
        self.tcp_server.pending_action_id = action_id
        self.tcp_server.pending_action_is_result = False

    def action_result(self, action_id, status, has_result):
        if not has_result:
            # Unity had nothing to send, so no message follows this command.
            self.tcp_server.unity_tcp_sender.send_unity_action_result(action_id, None, status)
            return
        self.tcp_server.pending_action_id = action_id
        self.tcp_server.pending_action_is_result = True
        self.tcp_server.pending_action_status = status

    def _remove(self, table, topic, kind):
        """
        Shared body of the four unregistration commands.

        Tolerates a topic that is not registered: Unity may drop an object whose
        registration never completed (or send the removal twice), and that is
        not worth failing the call over.
        """
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't remove a blank topic name! SysCommand.remove_{}".format(kind)
            )
            return

        old_node = table.pop(topic, None)
        if old_node is None:
            self.tcp_server.logwarn(
                "Remove{}({}) - not registered, ignoring".format(kind, topic)
            )
            return

        self.tcp_server.unregister_node(old_node)
        self.tcp_server.loginfo("Remove{}({}) OK".format(kind, topic))

    def remove_subscriber(self, topic):
        self._remove(self.tcp_server.subscribers_table, topic, "Subscriber")

    def remove_publisher(self, topic):
        self._remove(self.tcp_server.publishers_table, topic, "Publisher")

    def remove_ros_service(self, topic):
        self._remove(self.tcp_server.ros_services_table, topic, "RosService")

    def remove_unity_service(self, topic):
        self._remove(self.tcp_server.unity_services_table, topic, "UnityService")

    def response(self, srv_id):  # the next message is a service response
        self.tcp_server.pending_srv_id = srv_id
        self.tcp_server.pending_srv_is_request = False

    def request(self, srv_id):  # the next message is a service request
        self.tcp_server.pending_srv_id = srv_id
        self.tcp_server.pending_srv_is_request = True

    def topic_list(self):
        self.tcp_server.unity_tcp_sender.send_topic_list()

    def resolve_message_name(self, name, extension="msg"):
        try:
            names = name.split("/")
            module_name = names[0]
            class_name = names[1]
            importlib.import_module(module_name + "." + extension)
            module = sys.modules[module_name]
            if module is None:
                self.tcp_server.logerr("Failed to resolve module {}".format(module_name))
            module = getattr(module, extension)
            if module is None:
                self.tcp_server.logerr(
                    "Failed to resolve module {}.{}".format(module_name, extension)
                )
            module = getattr(module, class_name)
            if module is None:
                self.tcp_server.logerr(
                    "Failed to resolve module {}.{}.{}".format(module_name, extension, class_name)
                )
            return module
        except (IndexError, KeyError, AttributeError, ImportError) as e:
            self.tcp_server.logerr("Failed to resolve message name: {}".format(e))
            return None
