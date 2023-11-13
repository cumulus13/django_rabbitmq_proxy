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
import argparse
# from configset import configset

async def send_to_rabbitmq(message, severity = 'DEBUG', type_name='fanout', durable=True):
    try:
        transport, protocol = await aioamqp.connect(
            host=os.getenv('RABBITMQ_HOST_DJANGO') or 'localhost', port=os.getenv('RABBITMQ_PORT_DJANGO') or 5672,
            login=os.getenv('RABBITMQ_USERNAME_DJANGO') or 'syslog', password=os.getenv('RABBITMQ_PASSWORD_DJANGO') or 'DTPdev@2022@',
            virtualhost=os.getenv('RABBITMQ_VHOST_DJANGO') or '/', loop=asyncio.get_event_loop(),
        )
        
        exchange_name = 'django'

        channel = await protocol.channel()
        if severity in ('DEBUG', 'debug') or severity == 7 or severity == '7':
            await channel.exchange_declare(
                exchange_name='django', type_name=type_name, durable=durable
            )
        elif severity == 'INFO' or severity == 6 or severity == '6':
            exchange_name = 'django_info'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name=type_name, durable=durable
            )
        elif severity == 'NOTICE' or severity == 5 or severity == '5':
            exchange_name = 'django_notice'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name=type_name, durable=durable
            )
        elif severity == 'WARNING' or severity == 4 or severity == '4':
            exchange_name = 'django_warning'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name=type_name, durable=durable
            )
        elif severity == 'ERROR' or severity == 3 or severity == '3':
            exchange_name = 'django_error'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name=type_name, durable=durable
            )
        elif severity == 'CRITICAL' or severity == 2 or severity == '2':
            exchange_name = 'django_critical'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name=type_name, durable=durable
            )
        elif severity == 'ALERT' or severity == 1 or severity == '1':
            exchange_name = 'django_alert'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name=type_name, durable=durable
            )
        elif severity == 'EMERGENCY' or severity == 0 or severity == '' or severity == 'EMERG':
            exchange_name = 'django_emergency'
            await channel.exchange_declare(
                exchange_name=exchange_name, type_name=type_name, durable=durable
            )
        else:
            await channel.exchange_declare(
                exchange_name='django', type_name=type_name, durable=durable
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
    def __init__(self, loop, rabbit_host = '127.0.0.1', rabbit_port = 5672, type_name = 'fanout', durable = True):
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

async def start_udp_server(host: str = "0.0.0.0", port: int = 520, rabbit_host:str = '127.0.0.1', rabbit_port:int = 5672, type_name:str = 'fanout', durable:bool = True):
    loop = asyncio.get_running_loop()
    host = os.getenv('RABBITMQ_LISTEN_HOST_DJANGO') or host or "0.0.0.0"
    port = os.getenv('RABBITMQ_LISTEN_PORT_DJANGO') or port or 520
    print(
        make_colors("server listen on", 'lg') + " " + \
        make_colors(host, 'b', 'ly') + ":" + \
        make_colors(str(port), 'b', 'lc')
    )
    
    # def __init__(self, loop, rabbit_host = '127.0.0.1', rabbit_port = 5672, type_name = 'fanout', durable = True):
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: MyProtocol(loop, rabbit_host, rabbit_port, type_name, durable),
        local_addr=(host, port)
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

async def main(host: str = '0.0.0.0', port: int = 520, rabbit_host:str = '127.0.0.1', rabbit_port:int = 5672, type_name:str = 'fanout', durable:bool = True):
    await start_udp_server(host, port, rabbit_host, rabbit_port, type_name, durable)

def usage():
    
    
    print("""
         _____           _               _____                     
        / ____|         | |             |  __ \                    
       | (___  _   _ ___| | ___   __ _  | |__) | __ _____  ___   _ 
        \___ \| | | / __| |/ _ \ / _` | |  ___/ '__/ _ \ \/ / | | |
        ____) | |_| \__ \ | (_) | (_| | | |   | | | (_) >  <| |_| |
       |_____/ \__, |___/_|\___/ \__, | |_|   |_|  \___/_/\_\\__,  |
                __/ |             __/ |                       __/ |
               |___/             |___/                       |___/ 
    """)
    
    print("""
       _ _            _                __                   _                          __               _     _     _ _                   
      | (_)          | |               \ \                 | |                         \ \             | |   | |   (_) |                  
   ___| |_  ___ _ __ | |_   ______ _____\ \   ___ _   _ ___| | ___   __ _   ______ _____\ \   _ __ __ _| |__ | |__  _| |_ _ __ ___   __ _ 
  / __| | |/ _ \ '_ \| __| |______|______> > / __| | | / __| |/ _ \ / _` | |______|______> > | '__/ _` | '_ \| '_ \| | __| '_ ` _ \ / _` |
 | (__| | |  __/ | | | |_               / /  \__ \ |_| \__ \ | (_) | (_| |              / /  | | | (_| | |_) | |_) | | |_| | | | | | (_| |
  \___|_|_|\___|_| |_|\__|             /_/   |___/\__, |___/_|\___/ \__, |             /_/   |_|  \__,_|_.__/|_.__/|_|\__|_| |_| |_|\__, |
                                                   __/ |             __/ |                                                             | |
                                                  |___/             |___/                                                              |_|
""")

    parser = argparse.ArgumentParser()
    parser.add_argument('-P', '--port', action='store', help = 'Port listen on, default = 520', default = 520, type = int)
    parser.add_argument('-H', '--host', action='store', help = 'Host listen on, default = 0.0.0.0', default = '0.0.0.0')
    parser.add_argument('-rh', '--rabbit-host', action='store', help = 'RabbitMq host name / IP, default = 127.0.0.1', default = '127.0.0.1')
    parser.add_argument('-rp', '--rabbit-port', action='store', help = 'RabbitMq port, default = 5672', default = 5672, type = int)
    parser.add_argument('-t', '--exchange-type', action='store', help = 'RabbitMq exchange type, default = fanout', default = 'fanout')
    parser.add_argument('-d', '--durable', action='store_true', help = 'RabbitMq set durable')
    
    if len(sys.argv) == 1:
        parser.print_help()
        asyncio.run(main())
    else:
        args = parser.parse_args()
        asyncio.run(main(args.host, args.port, args.rabbit_host, args.rabbit_port, args.exchange_type, args.durable))
    

if __name__ == '__main__':
    # asyncio.run(main())
    # rabbitmq host test: 202.43.168.243
    usage()