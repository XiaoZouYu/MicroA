from __future__ import absolute_import

from mia.config import AMQP_URL_CONFIG_KEY


class Command(object):
    name = None
    __slots__ = ()

    @staticmethod
    def init_parser(parser):
        raise NotImplementedError

    @staticmethod
    def main(args, *unknown_args):
        raise NotImplementedError


def add_argument_config(parser):
    # yaml文件地址，默认为空。
    parser.add_argument(
        '-C',
        '--config',
        default='',
        type=str,
        dest='config',
        help='yaml文件地址，默认为空。'
    )


def add_argument_rabbit(parser):
    # rabbit地址，默认为空。
    parser.add_argument(
        '-R',
        '--rabbit',
        default='',
        type=str,
        dest=AMQP_URL_CONFIG_KEY,
        help='rabbit地址。如果要使用 `rabbit` 通信，那么这个参数必须存在与 `config文件` 或者命令行之一。\n'
             '传递样式格式为：`amqp://guest:guest@localhost:port`。'
    )


def add_argument_services(parser):
    # services 服务列表。
    parser.add_argument(
        'services',
        nargs='+',
        metavar='module',
        help='运行一个或多个服务类的Python路径。'
    )


def add_argument_host(parser):
    parser.add_argument(
        '-H',
        '--host',
        default='127.0.0.1',
        type=str,
        dest='host',
        help='主机地址，默认使用 `127.0.0.1`。'
    )


def add_argument_port(parser):
    parser.add_argument(
        '-P',
        '--port',
        default=8888,
        type=str,
        dest='port',
        help='端口地址，默认使用 `8888`。'
    )


class Run(Command):
    """运行 `mia` 服务。

    给定一个包含一个或多个 `mia` 服务的模块的python路径，将托管并运行它们。
    默认情况下，这将尝试找到看起来像服务的类(任何具有 `mia` 入口点的类)，但可以通过指定特定的服务
    `mia run module`.

    """

    name = 'run'

    @staticmethod
    def init_parser(parser):
        add_argument_config(parser)
        add_argument_rabbit(parser)
        add_argument_services(parser)
        add_argument_host(parser)
        add_argument_port(parser)

        return parser

    @staticmethod
    def main(args, *unknown_args):
        from .run import main
        main(args)


commands = Command.__subclasses__()
