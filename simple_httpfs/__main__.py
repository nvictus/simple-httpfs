import os.path as op
import argparse
import sys
from fuse import FUSE
from .httpfs import HttpFs


def main():
    parser = argparse.ArgumentParser(
        description="""usage: simple-httpfs <mountpoint>""")

    parser.add_argument('mountpoint')

    parser.add_argument(
        '-f', '--foreground',
        action='store_true',
        default=False,
        help='Run in the foreground')

    parser.add_argument(
        '--schema',
        default=None,
        type=str)

    parser.add_argument(
        '--disk-cache-size',
        default=2**30,
        type=int)

    parser.add_argument(
        '--disk-cache-dir',
        default='/tmp/xx')

    parser.add_argument(
        '--lru-capacity',
        default=400,
        type=int)

    args = vars(parser.parse_args())

    if not op.isdir(args['mountpoint']):
        print("Mount point must be a directory: {}".format(args['mountpoint']),
              file=sys.stderr)
        sys.exit(1)

    if args['schema'] is None:
        schema = op.split(args['mountpoint'].strip('/'))[-1]
    else:
        schema = args['schema']

    if schema not in ['http', 'https', 'ftp']:
        print('Could not infer schema. Try specifying either http, '
              'https or ftp using the --schema argument',
               file=sys.stderr)
        sys.exit(1)

    start_msg = """
Mounting HTTP Filesystem...
    schema: {schema}
    mountpoint: {mountpoint}
    foreground: {foreground}
""".format(schema=schema,
           mountpoint=args['mountpoint'],
           foreground=args['foreground'])
    print(start_msg, file=sys.stderr)

    fuse = FUSE(
        HttpFs(
               schema,
               disk_cache_size=args['disk_cache_size'],
               disk_cache_dir=args['disk_cache_dir'],
               lru_capacity=args['lru_capacity']
            ),
        args['mountpoint'],
        foreground=args['foreground']
    )


if __name__ == "__main__":
    main()
