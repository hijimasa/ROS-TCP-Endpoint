# Changelog

All notable changes to this repository will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## Unreleased

### Upgrade Notes

### Known Issues

### Added

Added Sonarqube scanner

Implemented the four unregistration system commands the ROS-TCP-Connector already
knew how to send but that had no handler here: `__remove_subscriber`,
`__remove_publisher`, `__remove_ros_service` and `__remove_unity_service`. Each
destroys the corresponding ROS node and drops it from its table; removing a topic
that is not registered logs a warning instead of failing, so a duplicate or
late removal is harmless. Without these, a Unity scene that despawns objects
leaked a ROS node per topic for the lifetime of the endpoint, and the topics
stayed visible in `ros2 topic list` long after their publisher was gone.
Verified against Unity_ROS2_Robot_Simulator: spawning a robot registers its
eight topics, despawning it removes all eight, and respawning brings them back.

### Changed

`SysCommands` dispatch now goes through an explicit allow list (`SysCommands.COMMANDS`)
rather than a bare `getattr` over every attribute of the class.

### Deprecated

### Removed

### Fixed

An unknown or malformed system command no longer takes down the whole client
connection. `handle_syscommand` used `getattr` without a default, so a command
this endpoint did not implement raised `AttributeError`; `ClientThread.run` only
catches `IOError`, so the exception reached its `finally` block and closed the
socket. From Unity's side every topic and every service went silent at once.
Unknown commands, undecodable payloads and wrong argument sets are now reported
back to Unity and logged, and the connection stays up.

`TcpServer.executor` is now initialised to `None` in the constructor. `start()`
spawns the listen thread before `setup_executor()` assigns the attribute, so a
client that connected in that window hit `AttributeError` and lost its
connection thread.

`default_server_endpoint.main` no longer exits with a traceback on Ctrl-C. The
`rclpy` signal handler has already shut the context down by then, so the
unconditional `rclpy.shutdown()` raised `RCLError: rcl_shutdown already called`;
it now uses the idempotent `rclpy.try_shutdown()`.


## [0.7.0] - 2022-02-01

### Added

Added Sonarqube scanner

Send information during hand shaking for ros and package version checks

Send service response as one queue item


## [0.6.0] - 2021-09-30

Add the [Close Stale Issues](https://github.com/marketplace/actions/close-stale-issues) action

### Upgrade Notes

### Known Issues

### Added

Support for queue_size and latch for publishers. (https://github.com/Unity-Technologies/ROS-TCP-Endpoint/issues/82)

### Changed

### Deprecated

### Removed

### Fixed

## [0.5.0] - 2021-07-15

### Upgrade Notes

Upgrade the ROS communication to support ROS2 with Unity

### Known Issues

### Added

### Changed

### Deprecated

### Removed

### Fixed

## [0.4.0] - 2021-05-27

Note: the logs only reflects the changes from version 0.3.0

### Upgrade Notes

RosConnection 2.0: maintain a single constant connection from Unity to the Endpoint. This is more efficient than opening one connection per message, and it eliminates a whole bunch of user issues caused by ROS being unable to connect to Unity due to firewalls, proxies, etc.

### Known Issues

### Added

Add a link to the Robotics forum, and add a config.yml to add a link in the Github Issues page

Add linter, unit tests, and test coverage reporting

### Changed

Improving the performance of the read_message in client.py, This is done by receiving the entire message all at once instead of reading 1024 byte chunks and stitching them together as you go.

### Deprecated

### Removed

Remove outdated handshake references

### Fixed
