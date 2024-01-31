from __future__ import print_function

import argparse

from mia.cli import commands
from pkg_resources import get_distribution
from mia.exceptions import ConfigurationError, ImportServiceError


def setup_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-V',
        '--version',
        action='version',
        version=get_distribution('mia').version
    )

    subparsers = parser.add_subparsers()

    for command in commands.commands:
        command_parser = subparsers.add_parser(
            command.name, description=command.__doc__)
        command.init_parser(command_parser)
        command_parser.set_defaults(main=command.main)

    return parser


def main():
    parser = setup_parser()
    args = parser.parse_args()
    try:
        args.main(args)
    except (ConfigurationError, ImportServiceError) as exc:
        print("错误运行: {}".format(exc))
