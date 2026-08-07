#!/usr/bin/env python

import rclpy

from ros_tcp_endpoint import TcpServer


def main(args=None):
    rclpy.init(args=args)
    tcp_server = TcpServer("UnityEndpoint")

    tcp_server.start()

    try:
        tcp_server.setup_executor()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            tcp_server.destroy_nodes()
        except Exception:
            # Already-destroyed nodes must not mask the shutdown below.
            pass
        # rclpy's own signal handler shuts the context down on Ctrl-C, so a
        # plain rclpy.shutdown() here raises "rcl_shutdown already called" and
        # the process exits with a traceback and a non-zero status. try_shutdown
        # is the idempotent form.
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
