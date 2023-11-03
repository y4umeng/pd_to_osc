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
        self._dispatcher.map("/index", self.__index_handler)
        self._dispatcher.map("/step", self.__step_handler)
        self.index = 0
        self.step = 1

    def __rate_handler(self, address, *args):
        # Modified from https://python-osc.readthedocs.io/en/latest/dispatcher.html
        if not len(args) == 1 or not address=="/rate" or (type(args[0]) is not int and type(args[0]) is not float):
            return
        self.rate = max(args[0], 0)

    def __index_handler(self, address, *args):
        if not len(args) == 1 or not address=="/index" or (type(args[0]) is not int and type(args[0]) is not float):
            return
        elif args[0] < 0 or args[0] >= len(self.df):
            print(f"Index must be in the range [0, {len(self.df) - 1}]")
        elif args[0] < 1:
            self.index = args[0] * (len(self.df) - 1)
        else: 
            self.index = args[0]

    def __step_handler(self, address, *args):
        if not len(args) == 1 or not address=="/step" or (type(args[0]) is not int and type(args[0]) is not float):
            return
        self.step = args[0]

    # async def __send_loop (self, columns, client):
        """
            old implementation for reference
        """
    #     for _, row in self.df.iloc[:].iterrows():
    #         for col in columns:
    #             client.send_message(f"/{col}", row[col])
    #         while self.rate == 0: await asyncio.sleep(.001)
    #         await asyncio.sleep(1/self.rate)

    #     await self.__send_loop(columns, client)

    # async def __send_loop_timed(self, columns, time_col, client, format):
        """
            old implementation for reference
        """
    #     last_time = datetime.datetime.strptime(self.df[time_col].values[0], format)
    #     for _, row in self.df.iloc[:].iterrows():
    #         curr_time = datetime.datetime.strptime(row[time_col] + "000", format) # 000 may not be necessary see datetime docs
    #         while self.rate == 0: await asyncio.sleep(.001)
    #         await asyncio.sleep(((curr_time - last_time).total_seconds())/self.rate) # figure out what rate means here like mathematicaly
    #         last_time = curr_time

    #         for col in columns:
    #             client.send_message(f"/{col}", row[col])

    
    async def __send_items(self, columns, client):
        """
            Functional version of __send_loop()
        """
        for col in columns:
            client.send_message(f"/{col}", self.df.iloc[[self.index]][col])
        while self.rate == 0: await asyncio.sleep(.001)
        await asyncio.sleep(1/self.rate)
        self.index = (self.step + self.index) % len(self.df)

    async def __send_items_timed(self, columns, time_col, client, format, last_time):
        """
            Functinal version of __send_loop_timed()
        """
        curr_time = datetime.datetime.strptime(self.df.iloc[[self.index]][time_col].iat[0] + "000", format) # 000 may not be necessary see datetime docs
        for col in columns:
            client.send_message(f"/{col}", self.df.iloc[[self.index]][col])
        while self.rate == 0: await asyncio.sleep(.001)
        await asyncio.sleep(((curr_time - last_time).total_seconds())/self.rate) # figure out what rate means here like mathematicaly
        self.index = (self.step + self.index) % len(self.df)
        return curr_time


    async def send_by_rate(self, send_port, columns, rate_init, receive_port):
        '''
            send_port: int
            columns: array of strings
            rate_init: int (number of rows per second)
            receive_port: int

        '''

        client = SimpleUDPClient(self._ip, send_port)
        self.rate = rate_init

        if receive_port is not None:
            server = AsyncIOOSCUDPServer((self._ip, receive_port), self._dispatcher, asyncio.get_event_loop())
            transport, _ = await server.create_serve_endpoint()  # Create datagram endpoint and start serving

        try: 
            while True:
                await self.__send_items(columns, client)
        except:
            try: transport.close()  # Clean up serve endpoint
            except: pass

    async def send_by_time(self, send_port, columns, rate_init, receive_port, time_col, format):
        """
            send_port: int
            columns: array of strings
            rate_init: int 
            receive_port: int
            time_col: string
            format: string

        """
        client = SimpleUDPClient(self._ip, send_port)

        self.rate = rate_init

        if receive_port is not None:
            server = AsyncIOOSCUDPServer((self._ip, receive_port), self._dispatcher, asyncio.get_event_loop())
            transport, _ = await server.create_serve_endpoint()  # Create datagram endpoint and start serving

        last_time = datetime.datetime.strptime(self.df[time_col].values[self.index], format) 

        try: 
            while True:
                last_time = await self.__send_items_timed(columns, time_col, client, format, last_time)
        except:
            try: transport.close()  # Clean up serve endpoint 
            except: return
        

# test

cl = ClientServer("127.0.0.1", "catalog.csv")
print(cl.df.columns)

format = "%Y-%m-%d %H:%M:%S.%f"

# asyncio.run(cl.send_by_rate(8888, ["latitude", "longitude"], 3, 7777))

asyncio.run(cl.send_by_time(8888, ["latitude"], 10000, 7777, "origin_time", format))

# cl =  ClientServer("127.0.0.1", "customers-100.csv") 
# print(cl.df.columns)
# asyncio.run(cl.send_by_rate(8888, ["Index"], 3, 7777))

