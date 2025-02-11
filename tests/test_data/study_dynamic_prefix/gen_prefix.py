import argparse

import rich

parser = argparse.ArgumentParser()
parser.add_argument("--prefix", default="dynamic")
args, _rest = parser.parse_known_args()

rich.print(args.prefix)
