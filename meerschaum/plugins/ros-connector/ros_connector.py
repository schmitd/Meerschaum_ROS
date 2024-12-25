#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

@make_connector
class ROS2Connector(Connector):
    """
    Ingest data from topics.
    """
    from ._subscribe import subscribe, _subscribe_on_connect, _on_message,
    from ._publish import publish
    from ._fetch import fetch, get_topics_from_pipe

    @property
    def topics(self) -> Dict[str,int]:
        """
        Return the of currently subscribed topics.
        """
        _topics = self.__dict__.get('_topics', None)
        if _topics is None:
            _topics = {}
            self._topics = _topics
        return _topics

    @property
    def client(self) -> 'rcl.client.Client':
        """
        Return a rcl service client.
        """
        _client = self.__dict__.get('_client', None)
        if _client is not None:
            return _client
        self._client = self.build_client()
        return self._client

    @property
    def publisher(self) -> 'rcl.publisher.Publisher':
        """
        Return a rcl publisher.
        """
        _publisher = self.__dict__.get('_publisher', None)
        if _publisher is not None:
            return _publisher
        self._publisher = build_publisher()
        return self._publisher

    @property
    def subscriber(self) -> 'rcl.subscriber.Subscriber':
        """
        Return a rcl subscriber.
        """
        _subscriber = self.__dict__.get('_publish_client', None)
        if _subscriber is not None:
            return _subscriber
        self._subscriber = build_subscriber() 
        return self._subscriber

    def build_publisher(self) -> 'rcl.publisher.Publisher':
        with mrsm.Venv('ros2-connector'):
            from rcl.node import Node
            _publisher = Node.create_publisher()
        return _publisher

    def build_subscriber(self) -> 'rcl.publisher.Publisher':
        with mrsm.Venv('ros2-connector'):
            from rcl.node import Node
            _subscriber = Node.create_subscriber()
        return _subscriber

    def build_client(self) -> 'rcl.client.Client':
        with mrsm.Venv('ros2-connector'):
            from rclpy.node import Node
            _client = Node.create_client()
        return _client

    def __del__(self) -> None:
        """
        Disconnect the client before deletion.
        """
        _clients = [
            self.__dict__.get('_client', None),
            self.__dict__.get('_subscriber', None),
            self.__dict__.get('_publisher', None),
        ]
        for _client in _clients:
            if _client is not None:
                _client.destroy_node()

    @property
    def pool(self) -> 'multiprocessing.pool.ThreadPool':
        """
        Return the ThreadPool to use for callbacks.
        """
        _pool = self.__dict__.get('_pool', None)
        if _pool is not None:
            return _pool
        self._pool = get_pool()
        return self._pool

class MinimalSubscriber(Node):

    def __init__(self):
        super().__init__('minimal_subscriber')
        self.subscription = self.create_subscription(
            String,
            'topic',
            self.listener_callback,
            10)
        self.subscription  # prevent unused variable warning

    def listener_callback(self, msg):
        self.get_logger().info('I heard: "%s"' % msg.data)
