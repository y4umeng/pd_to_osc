from pythonosc.udp_client import SimpleUDPClient
from pythonosc.osc_server import AsyncIOOSCUDPServer
from pythonosc.dispatcher import Dispatcher
import asyncio
import pandas as pd
import datetime

class ClientServer:
    _ip: str
    df: pd.DataFrame
    rate: int
    _dispatcher: Dispatcher

    def __init__ (self, IP, filepath):
        self._ip = IP
        self.df = pd.read_csv(filepath)
        self.rate = 0
        self._dispatcher = Dispatcher()
        self._dispatcher.map("/rate", self.__rate_handler)

    def __rate_handler(self, address, *args):
        # Modified from https://python-osc.readthedocs.io/en/latest/dispatcher.html
        if not len(args) == 1 or not address=="/rate" or (type(args[0]) is not int and type(args[0]) is not float):
            return
        self.rate = args[0]


    async def __send_loop (self, columns, client):
        for _, row in self.df.iterrows():
            for col in columns:
                client.send_message(f"/{col}", row[col])
            while self.rate == 0: await asyncio.sleep(.001)
            await asyncio.sleep(1/self.rate)

    async def __send_loop_timed(self, columns, time_col, client, format):
        last_time = datetime.datetime.strptime(self.df[time_col].values[0], format)
        for _, row in self.df.iterrows():
            curr_time = datetime.datetime.strptime(row[time_col] + "000", format) # 000 may not be necessary see datetime docs
            while self.rate == 0: await asyncio.sleep(.001)
            await asyncio.sleep(((curr_time - last_time).total_seconds())/self.rate) # figure out what rate means here like mathematicaly
            last_time = curr_time

            for col in columns:
                client.send_message(f"/{col}", row[col])

    async def send_by_rate(self, port, columns, rate_init, rate_port):
        '''
            port: int
            columns: array of strings
            rate_init: int (number of rows per second)
            rate_port: int

        '''

        client = SimpleUDPClient(self._ip, port)
        self.rate = rate_init

        if rate_port is None:
            await self.__send_loop(columns, client)
        else:
            server = AsyncIOOSCUDPServer((self._ip, rate_port), self._dispatcher, asyncio.get_event_loop())
            transport, _ = await server.create_serve_endpoint()  # Create datagram endpoint and start serving

            await self.__send_loop(columns, client)  # Enter main loop of program

            transport.close()  # Clean up serve endpoint

    async def send_by_time(self, port, columns, rate_init, rate_port, time_col, format):
        """
            port: int
            columns: array of strings
            rate_init: int 
            rate_port: int
            time_col: string
            format: string

        """
        client = SimpleUDPClient(self._ip, port)
        self.rate = rate_init

        if rate_port is None:
            await self.__send_loop_timed(columns, time_col, client, format)  # Enter main loop of program
        else:
            server = AsyncIOOSCUDPServer((self._ip, rate_port), self._dispatcher, asyncio.get_event_loop())
            transport, _ = await server.create_serve_endpoint()  # Create datagram endpoint and start serving

            await self.__send_loop_timed(columns, time_col, client, format)  # Enter main loop of program

            transport.close()  # Clean up serve endpoint 
        

# test

cl = ClientServer("127.0.0.1", "catalog.csv")
print(cl.df.columns)

format = "%Y-%m-%d %H:%M:%S.%f"

# asyncio.run(cl.send_by_rate(8888, ["latitude", "longitude"], 3, 7777))

asyncio.run(cl.send_by_time(8888, ["latitude"], 10000, 7777, "origin_time", format))