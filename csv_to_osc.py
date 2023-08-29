from pythonosc.udp_client import SimpleUDPClient
import pandas as pd
import time
import datetime

class Client:
    ip: str
    df: pd.DataFrame

    def __init__ (self, IP, filepath):
        self.ip = IP
        self.df = pd.read_csv(filepath)

    def send_by_rate(self, ports, rate, columns):
        '''
            ports: array of ints
            rate: int (number of rows per second)
            columns: array of strings

        '''

        clients = {col: SimpleUDPClient(self.ip, port) for col, port in zip(columns, ports)}

        for _, row in self.df.iterrows():
            for col in columns:
                clients[col].send_message("/", row[col])
            time.sleep(1/rate)


    def send_by_time(self, ports, rate, columns, time_col, format):
        """
            ports: array of ints
            rate: int 
            columsn: array of strings
            time: string
            format: string

        """
        last_time = datetime.datetime.strptime(self.df[time_col].values[0], format)
        clients = {col: SimpleUDPClient(self.ip, port) for col, port in zip(columns, ports)}
        for _, row in self.df.iterrows():
            curr_time = datetime.datetime.strptime(row[time_col] + "000", format) # 000 may not be necessary see datetime docs
            time.sleep(((curr_time - last_time).total_seconds())/rate)
            last_time = curr_time

            for col in columns:
                clients[col].send_message("/", row[col])
            
            
        

cl = Client("127.0.0.1", "catalog.csv")
print(cl.df.columns)

format = "%Y-%m-%d %H:%M:%S.%f"
# cl.send_by_rate([8888], 3, ["origin_time"])

cl.send_by_time([8888], 10000, ["latitude"], "origin_time", format)

