import argparse
from src.loader import RFBLoader

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--load-all", action="store_true")

    args = parser.parse_args()

    if args.load_all:
        RFBLoader().run()