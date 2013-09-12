import argparse
import logging

from s3tup import s3tup

parser = argparse.ArgumentParser(description='s3tup')
parser.add_argument(
	'config',
	help='path to your configuration file'
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
	help='run in rsync only mode'
)
verbosity_group = parser.add_mutually_exclusive_group()
verbosity_group.add_argument(
	"-v",
	"--verbose",
	action="store_true",
    help="increase output verbosity"
)
verbosity_group.add_argument(
	"-q",
	"--quiet",
	action="store_true",
    help="silence all output"
)

def main():
	args = parser.parse_args()

	if args.quiet:
		# Silence all logging
		pass
	elif args.verbose:
		# Verbose mode, log level = debug
		logging.basicConfig(format="%(levelname)s: %(message)s",
							level=logging.DEBUG)
	else:
		# Default, log level = info
		logging.basicConfig(format="%(message)s", level=logging.INFO)
	
	s3tup(args.config, args.access_key_id, args.secret_access_key,
		  args.rsync_only)