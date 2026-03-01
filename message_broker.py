import asyncio
from typing import Optional, TypeVar, Union

from aio_pika import Message, RobustConnection, connect_robust
from aio_pika.abc import AbstractConnection, ExchangeType, TimeoutType
from aiormq.abc import ConfirmationFrameType

from config import settings

ConnectionType = TypeVar("ConnectionType", bound=AbstractConnection)


class RabbitMQ:
    def __init__(
        self,
        url: str = None,
        *,
        host: str = "localhost",
        port: int = 5672,
        login: str = "guest",
        password: str = "guest",
        virtualhost: str = "/",
        ssl: bool = False,
        loop: asyncio.AbstractEventLoop = None,
        ssl_options: dict = None,
        timeout: TimeoutType = None,
        connection_class: ConnectionType = RobustConnection,
        client_properties: dict = None,
        # exchangers: Dict[str, Dict[str, Any]] = None,
        **kwargs,
    ):
        self.url = url
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.virtualhost = virtualhost
        self.ssl = ssl
        self.loop = loop
        self.ssl_options = ssl_options
        self.timeout = timeout
        self.connection_class = connection_class
        self.client_properties = client_properties
        # self.exchangers = exchangers or {}
        self.kwargs = kwargs

    async def connect(self):
        host = settings.RABBITMQ_HOST
        username = settings.RABBITMQ_USERNAME
        password = settings.RABBITMQ_PASSWORD
        port = settings.RABBITMQ_PORT
        vhost = settings.RABBITMQ_VHOST

        self.connection = await connect_robust(
            self.url,
            host=host,
            port=port,
            login=username,
            password=password,
            virtualhost=vhost,
            ssl=self.ssl,
            loop=self.loop,
            ssl_options=self.ssl_options,
            timeout=self.timeout,
            connection_class=self.connection_class,
            client_properties=self.client_properties,
            **self.kwargs,
        )
        return self

    def __await__(self):
        return self.connect().__await__()

    async def include_exchange(
        self,
        name: str,
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        durable: bool = None,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None,
    ):
        async with self.connection.channel() as channel:
            await channel.declare_exchange(
                name, type, durable, auto_delete, internal, passive, arguments, timeout
            )

    async def publish(
        self,
        message: Message,
        routing_key: str,
        exchange_name: str = "",
        *,
        mandatory: bool = True,
        immediate: bool = False,
        timeout: TimeoutType = None,
    ) -> Optional[ConfirmationFrameType]:
        async with self.connection.channel() as channel:
            if exchange_name:
                exchange = await channel.get_exchange(exchange_name)
                await exchange.publish(
                    message,
                    routing_key,
                    mandatory=mandatory,
                    immediate=immediate,
                    timeout=timeout,
                )
            return await channel.default_exchange.publish(
                message,
                routing_key,
                mandatory=mandatory,
                immediate=immediate,
                timeout=timeout,
            )
