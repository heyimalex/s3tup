import argparse
import logging
import sys
import os

from s3tup.parse import load_config, parse_config

log = logging.getLogger('s3tup')
title = (
    "\n"
    "        _____ __             \n"
    "   ____|__  // /___  ______  \n"
    "  / ___//_ </ __/ / / / __ \ \n"
    " /__  /__/ / /_/ /_/ / /_/ / \n"
    "/____/____/\__/\____/ /___/  \n"
    "                   /_/       \n"
)

parser = argparse.ArgumentParser(description=
    's3tup: Declarative configuration management and deployment tool for'
    ' Amazon S3')
parser.add_argument(
    'config',
    help='path to your configuration file')
parser.add_argument(
    '--dryrun',
    action='store_true',
    help='run s3tup without actually... running s3tup')
parser.add_argument(
    '--rsync_only',
    action='store_true',
    help='only upload/delete keys that have been modified/removed, don\'t'
         ' sync|redirect keys and don\'t sync bucket attributes')
parser.add_argument(
    '-c',
    metavar='CONCURRENCY',
    type=int,
    help='concurrency')
verbosity = parser.add_mutually_exclusive_group()
verbosity.add_argument(
    '-v', '--verbose',
    action='store_true',
    help='increase output verbosity')
verbosity.add_argument(
    '-q', '--quiet',
    action='store_true',
    help='silence all output')
parser.add_argument(
    '--access_key_id',
    help='your aws access key id. unnescessary if your AWS_ACCESS_KEY_ID env var is set.')
parser.add_argument(
    '--secret_access_key',
    help='your aws secret access key. unnescessary if your AWS_SECRET_ACCESS_KEY env var is set.')

def main():
    args = parser.parse_args()

    if not (args.quiet or args.verbose):
        logging.basicConfig(format='%(message)s', level=logging.INFO)
    elif args.verbose:
        logging.basicConfig(format='%(levelname)s: %(message)s',
                            level=logging.DEBUG)

    try:
        run(args.config, args.dryrun, args.rsync_only, args.c,
            args.access_key_id, args.secret_access_key)
    except Exception as e:
        if args.verbose:
            raise
        log.error('{}: {}'.format(sys.exc_info()[0].__name__, e))
        sys.exit(1)

def run(config, dryrun=False, rsync_only=False, concurrency=None,
        access_key_id=None, secret_access_key=None,):

    if access_key_id is not None:
        os.environ['AWS_ACCESS_KEY_ID'] = access_key_id
    if secret_access_key is not None:
        os.environ['AWS_SECRET_ACCESS_KEY'] = secret_access_key

    config = load_config(config)
    buckets = parse_config(config)

    log.info(title)

    for b in buckets:
        if concurrency:
            b.conn.concurrency = concurrency
        b.sync(dryrun=dryrun, rsync_only=rsync_only)