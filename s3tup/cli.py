import argparse
import logging

from s3tup import s3tup

parser = argparse.ArgumentParser(description='s3tup')
parser.add_argument(
	'config',
	help='path to configuration file'
	)
parser.add_argument(
	'--access_key_id',
	help='your aws access key id'
)
parser.add_argument(
	'--secret_access_key',
	help='your aws secret access key'
)
parser.add_argument(
	'--rsync_only',
	action='store_true',
	help='only run rsync, do not sync other keys'
)
verbosity_group = parser.add_mutually_exclusive_group()
verbosity_group.add_argument(
	"-v",
	"--verbose",
	action="store_true",
    help="run in verbose mode"
)
verbosity_group.add_argument(
	"--debug",
	action="store_true",
    help="run in debug mode"
)

def main():
	args = parser.parse_args()

	if args.debug:
		logging.basicConfig(format="%(levelname)s: %(message)s",
							level=logging.DEBUG)
	elif args.verbose:
		logging.basicConfig(format="%(message)s", level=logging.INFO)

	s3tup(args.config, args.access_key_id, args.secret_access_key,
		  args.rsync_only)