import asyncio
import json
import aioamqp
import ast
from jsoncolor import jprint
from  make_colors import make_colors
from unidecode import unidecode
import traceback
from pydebugger.debug import debug
import sys
import os
from configset import configset

configname = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'server.ini')
CONFIG = configset(configname)

async def send_to_rabbitmq(message, severity = 'DEBUG'):
    try:
        transport, protocol = await aioamqp.connect(
            host=os.getenv('RABBITMQ_HOST_DJANGO') or 'localhost', port=os.getenv('RABBITMQ_PORT_DJANGO') or 5672,
            login=os.getenv('RABBITMQ_USERNAME_DJANGO') or 'syslog', password=os.getenv('RABBITMQ_PASSWORD_DJANGO') or 'DTPdev@2022@',
            virtualhost=os.getenv('RABBITMQ_VHOST_DJANGO') or '/', loop=asyncio.get_event_loop(),
        )
        
        exchange_name = CONFIG.get_config('exchange', 'name') or 'django'

        channel = await protocol.channel()
        if severity in ('DEBUG', 'debug') or severity == 7 or severity == '7':
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name='fanout', durable=True
            )
        elif severity == 'INFO' or severity == 6 or severity == '6':
            exchange_name = exchange_name +  '_info'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name='fanout', durable=True
            )
        elif severity == 'NOTICE' or severity == 5 or severity == '5':
            exchange_name = exchange_name + '_notice'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name='fanout', durable=True
            )
        elif severity == 'WARNING' or severity == 4 or severity == '4':
            exchange_name = exchange_name + '_warning'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name='fanout', durable=True
            )
        elif severity == 'ERROR' or severity == 3 or severity == '3':
            exchange_name = exchange_name + '_error'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name='fanout', durable=True
            )
        elif severity == 'CRITICAL' or severity == 2 or severity == '2':
            exchange_name = exchange_name + '_critical'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name='fanout', durable=True
            )
        elif severity == 'ALERT' or severity == 1 or severity == '1':
            exchange_name = exchange_name + '_alert'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name='fanout', durable=True
            )
        elif severity == 'EMERGENCY' or severity == 0 or severity == '' or severity == 'EMERG':
            exchange_name = exchange_name + '_emergency'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name='fanout', durable=True
            )
        else:
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name='fanout', durable=True
            )

        #debug(message = message)
        if sys.version_info.major == 2:
            if hasattr(message, 'decode'): message = message.encode('utf-8')
        else:
            if not hasattr(message, 'decode'): message = bytes(message, encoding = "utf-8")
        
        
        await channel.basic_publish(
            payload=message,
            exchange_name=exchange_name,
            routing_key='',
        )
        
        #await channel.basic_publish(
            #payload=message,
            #exchange_name='django',
            #routing_key='',
        #)        

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
        try:
            message = data.decode('utf-8')
        except UnicodeDecodeError:
            try:
                message = unidecode(data)
            except:
                print(traceback.format_exc())
                message = data
        #print(f"Received message: {message}")
        error = False
        #debug(message = message)
        try:
            json_data = ast.literal_eval(message)
            json_data = ast.literal_eval(json_data)
            #debug(json_data = json_data)
            jprint(json_data)
        except:
            try:
                json_data = json.loads(message)
                print("ERROR 1")
                print(make_colors(json_data, 'b', 'lg'))
            except:
                print("ERROR 2")
                json_data = message
                print(make_colors(json_data, 'lw', 'r'))
                error = traceback.format_exc()
        if error:
            #asyncio.ensure_future(send_to_rabbitmq(json_data))
            
            thd = [send_to_rabbitmq(str(json_data), "ERROR"), send_to_rabbitmq(str(error), "ERROR")]
            tasks = [asyncio.ensure_future(coro) for coro in thd]
            asyncio.gather(*tasks)            
        else:
            debug(json_data = json_data)
            if not isinstance(json_data, str):
                thd = [send_to_rabbitmq(str(json_data), json_data.get('levelname')), send_to_rabbitmq(str(json_data))]
                tasks = [asyncio.ensure_future(coro) for coro in thd]
                asyncio.gather(*tasks)
            else:
                print("Data JSON is String/Text ! ..........")

    def error_received(self, exc):
        print('Error received:', exc)

    def connection_lost(self, exc):
        print('Closing transport')
        self.transport.close()

async def start_udp_server():
    loop = asyncio.get_running_loop()
    port = os.getenv('RABBITMQ_LISTEN_PORT_DJANGO') or 520
    print(
        make_colors("server listen on", 'lg') + " " + \
        make_colors("0.0.0.0", 'b', 'ly') + ":" + \
        make_colors(str(port), 'b', 'lc')
    )
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: MyProtocol(loop),
        local_addr=('0.0.0.0', port)
    )

    #try:
        #await asyncio.sleep(36000)  # Serve for 1 hour.
    #finally:
        #transport.close()
        #await transport.wait_closed()
    try:
        while True:
            await asyncio.sleep(3600)  # Sleep for 1 second.
    finally:
        transport.close()
        await transport.wait_closed()


async def main():
    await start_udp_server()

if __name__ == '__main__':
    asyncio.run(main())
