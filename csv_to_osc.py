import argparse
import os
from models import ClientServer

"""
    --filepath
    --ip (default local)
    --port
    --cols (default all)
    --time-col
    --format
    --rate
    --rate-ip (default to local ot if --ip is set to that)
    --rate-port

"""

parser = argparse.ArgumentParser(
    description="Process raw registration forms into a standard csv."
)

parser.add_argument(
    "--filepath", "-fp", type=str, help="File path for CSV", required=True
)

parser.add_argument(
    "--ip", "-ip", type=str, help="IP address to send csv data over.", default="127.0.0.1"
)

parser.add_argument(
    "--port", "-p", type=int, help="Port to send csv data over", required=True
)

parser.add_argument(
    "--columns", "--cols", "-c", nargs="+", help="CSV columns to send", required=True
)

parser.add_argument(
    "--time", "-t", type=str, help="Column containing time data."
)

parser.add_argument(
    "--format", "-f", type=str, help="Datetime format string"
)

parser.add_argument(
    "--rate", "--r", type=int, help="Rate at which data is sent. Messages/seconds.", default=5
)

parser.add_argument(
    "--rate_ip", "-ri", type=str, help="IP address to send live rate data over.", default="127.0.0.1"
)

parser.add_argument(
    "--rate_port", "-rp", type=int, help="Port to send rate data over.", default = None
)

parser.add_argument(
    "--infinite", "-inf", action="store_true", help="Infinite loop flag."
)



def main():
    args = parser.parse_args()
    print("Hello!")
    if not os.path.exists(args.filepath):
        raise FileNotFoundError(
            f"Could not find file: {args.filepath}. Please check that you spelled the file name "
            "correctly and are running the command from the correct directory."
        )
    
    cs = ClientServer(args.ip, args.filepath, )
    if args.time:
        if not args.format: 
            raise ValueError(print("Datetime format must be specified if a time column is provided."))
        cs.send_by_time(args.port, args.columns, args.rate, args.rate_port, args.time, args.format)
    else:
        cs.send_by_rate(args.port, args.columns, args.rate, args.rate_port)

