#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8


"""
Define methods for subscribing to ROS2 topics
"""

def subscribe(
        self,
        topic: str,
        callback: Callable[[Any], Any],
        blocking: bool = False,
        decode_payload: bool = True,
        debug: bool = False,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Subscribe to this connector's topic and fire a callback when a message is received.

    Parameters
    ----------
    topic: str
        The topic to subscribe to.

    callback: Callable[[Any], Any]
        A callback function to be fired when a message is received.
        Accepts the payload of the message.

    blocking: bool, default False
        If `True`, block execution at this method and loop forever to wait for messages.
        Otherwise use a background thread and continue execution.

    decode_payload: bool, default True
        If `True`, decode the message payload bytes as UTF-8-encoded JSON.
        Otherwise pass the raw bytes into the callback.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """

    topic_meta = self.topics.get(topic, {})

    self.subscribe_client.on_message = self._on_message
    self.subscribe_client.on_connect = self._subscribe_on_connect
    self.topics[topic] = {
        'qos': qos,
        'callback': callback,
        'regex': self.mqtt_topic_to_regex(topic),
        'parser_kwargs': {
            'decode_payload': decode_payload,
        },
    }

    try:
        self.subscribe_client.connect(self.host, self.port, self.keepalive)
    except Exception as e:
        message = f"Failed to connect to MQTT host:\n{traceback.format_exc()}"
        return False, message

    if not blocking:
        self.subscribe_client.loop_start()
    else:
        self.subscribe_client.loop_forever()

    return True, f"Subscribed to '{topic}' with quality-of-service level {qos}."


def _subscribe_on_connect(
        self,
        client: 'paho.mqtt.client.Client',
        userdata: 'paho.mqtt.client.MQTTMessageInfo',
        flags: Dict[str, int],
        return_code: int,
    ) -> None:
    """
    Subscribe to the topic upon connecting (in case of disconnects).
    """
    if return_code > 0:
        warn(
            f"[{self}] Received return code {return_code} from "
            + f"'{self.host}' on topic '{topic}'."
        )
        if return_code == 5:
            warn(f"Are the credentials for '{self}' correct?", stack=False)

    for topic, topic_meta in [_ for _ in self.topics.items()]:
        client.subscribe(topic, qos=topic_meta.get('qos', 0))


def _on_message(
        self,
        client: 'paho.mqtt.client.Client',
        userdata: 'paho.mqtt.client.MQTTMessageInfo',
        message: 'paho.mqtt.client.MQTTMessage',
    ) -> Any:
    """
    When messages are received, invoke the correct callback.
    """
    matched_topics = {
        subscribed_topic: subscribed_topic_meta
        for subscribed_topic, subscribed_topic_meta in self.topics.items()
        if subscribed_topic_meta['regex'].match(message.topic)
    }

    def parse_matched_topic(topic: str):
        topic_meta = matched_topics[topic]
        decode_payload = topic_meta['parser_kwargs']['decode_payload']
        callback = topic_meta['callback']
        payload = (
            json.loads(message.payload.decode('utf-8'))
            if decode_payload
            else message.payload
        )
        return callback(payload, **filter_keywords(callback, topic=message.topic))

    return self.pool.map(parse_matched_topic, matched_topics.keys())



@staticmethod
def mqtt_topic_to_regex(topic: str) -> re.Pattern:
    """
    Convert an MQTT topic to regex.
    """
    return re.compile(
        '^' + topic.replace('+', r'[^/]+').replace('#', r'.+') + '$'
    )
