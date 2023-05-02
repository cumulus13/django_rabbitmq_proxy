import asyncio
import json
import aioamqp
import ast

async def send_to_rabbitmq(message):
    try:
        transport, protocol = await aioamqp.connect(
            host='localhost', port=5672,
            login='syslog', password='DTPdev@2022@',
            virtualhost='/', loop=asyncio.get_event_loop(),
        )

        channel = await protocol.channel()
        await channel.exchange_declare(
            exchange_name='django', type_name='fanout', durable=True
        )

        message = message.encode('utf-8')
        await channel.basic_publish(
            payload=message,
            exchange_name='django',
            routing_key='',
        )

        await protocol.close()
        transport.close()
    except aioamqp.AmqpClosedConnection:
        print("closed connections")

class MyProtocol:
    def __init__(self, loop):
        self.loop = loop

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        message = data.decode('utf-8')
        print(f"Received message: {message}")
        try:
            json_data = json.loads(message)
        except:
            try:
                json_data = ast.literal_eval(message)
            except:
                json_data = message
        asyncio.ensure_future(send_to_rabbitmq(message))

    def error_received(self, exc):
        print('Error received:', exc)

    def connection_lost(self, exc):
        print('Closing transport')
        self.transport.close()

async def start_udp_server():
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: MyProtocol(loop),
        local_addr=('0.0.0.0', 520)
    )

    try:
        await asyncio.sleep(3600)  # Serve for 1 hour.
    finally:
        transport.close()
        await transport.wait_closed()

async def main():
    await start_udp_server()

if __name__ == '__main__':
    asyncio.run(main())
