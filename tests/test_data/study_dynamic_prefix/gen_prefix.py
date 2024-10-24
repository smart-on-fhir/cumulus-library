import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--prefix", default="dynamic")
args, _rest = parser.parse_known_args()

print(args.prefix)
