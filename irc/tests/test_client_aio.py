import asyncio
import contextlib
import warnings
from unittest.mock import MagicMock

import pytest

from irc import client_aio


def make_mocked_create_connection(mock_transport, mock_protocol):
    async def mock_create_connection(*args, **kwargs):
        return (mock_transport, mock_protocol)

    return mock_create_connection


@contextlib.contextmanager
def suppress_issue_197():
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', 'There is no current event loop')
        yield


def test_privmsg_sends_msg():
    # create dummy transport, protocol
    mock_transport = MagicMock()
    mock_protocol = MagicMock()

    # connect to dummy server
    with suppress_issue_197():
        loop = asyncio.get_event_loop()
    loop.create_connection = make_mocked_create_connection(
        mock_transport, mock_protocol
    )
    server = client_aio.AioReactor(loop=loop).server()
    loop.run_until_complete(server.connect('foo', 6667, 'my_irc_nick'))
    server.privmsg('#best-channel', 'You are great')

    mock_transport.write.assert_called_with(b'PRIVMSG #best-channel :You are great\r\n')

    loop.close()


def test_dcc_connection_connect():
    mock_transport = MagicMock()
    mock_protocol = MagicMock()

    loop = asyncio.new_event_loop()
    loop.create_connection = make_mocked_create_connection(
        mock_transport, mock_protocol
    )
    reactor = client_aio.AioReactor(loop=loop)
    dcc = reactor.dcc()
    loop.run_until_complete(dcc.connect('127.0.0.1', 12345))

    assert dcc.connected
    assert dcc.transport is mock_transport
    assert dcc.protocol is mock_protocol
    assert dcc.peeraddress == '127.0.0.1'
    assert dcc.peerport == 12345

    loop.close()


def test_dcc_connection_disconnect():
    mock_transport = MagicMock()
    mock_protocol = MagicMock()

    loop = asyncio.new_event_loop()
    loop.create_connection = make_mocked_create_connection(
        mock_transport, mock_protocol
    )
    reactor = client_aio.AioReactor(loop=loop)
    dcc = reactor.dcc()
    loop.run_until_complete(dcc.connect('127.0.0.1', 12345))
    dcc.disconnect('test message')

    mock_transport.close.assert_called_once()
    assert dcc not in reactor.connections
    # Calling disconnect again should be a no-op (idempotent)
    dcc.disconnect('test message again')
    mock_transport.close.assert_called_once()

    loop.close()


def test_dcc_connection_send_bytes():
    mock_transport = MagicMock()
    mock_protocol = MagicMock()

    loop = asyncio.new_event_loop()
    loop.create_connection = make_mocked_create_connection(
        mock_transport, mock_protocol
    )
    reactor = client_aio.AioReactor(loop=loop)
    dcc = reactor.dcc()
    loop.run_until_complete(dcc.connect('127.0.0.1', 12345))
    dcc.send_bytes(b'hello')

    mock_transport.write.assert_called_with(b'hello')

    loop.close()


def test_dcc_connection_send_bytes_not_connected():
    loop = asyncio.new_event_loop()
    reactor = client_aio.AioReactor(loop=loop)
    dcc = reactor.dcc()
    # Should not raise when not connected
    dcc.send_bytes(b'hello')
    loop.close()


def test_dcc_connection_process_data_chat():
    mock_transport = MagicMock()
    mock_protocol = MagicMock()

    loop = asyncio.new_event_loop()
    loop.create_connection = make_mocked_create_connection(
        mock_transport, mock_protocol
    )
    reactor = client_aio.AioReactor(loop=loop)
    dcc = reactor.dcc('chat')
    loop.run_until_complete(dcc.connect('127.0.0.1', 12345))

    events = []
    reactor.add_global_handler('dccmsg', lambda c, e: events.append(e))
    dcc.process_data(b'hello\nworld\n')

    assert len(events) == 2
    assert events[0].arguments == [b'hello']
    assert events[1].arguments == [b'world']

    loop.close()


def test_dcc_connection_process_data_raw():
    mock_transport = MagicMock()
    mock_protocol = MagicMock()

    loop = asyncio.new_event_loop()
    loop.create_connection = make_mocked_create_connection(
        mock_transport, mock_protocol
    )
    reactor = client_aio.AioReactor(loop=loop)
    dcc = reactor.dcc('raw')
    loop.run_until_complete(dcc.connect('127.0.0.1', 12345))

    events = []
    reactor.add_global_handler('dccmsg', lambda c, e: events.append(e))
    dcc.process_data(b'rawdata')

    assert len(events) == 1
    assert events[0].arguments == [b'rawdata']

    loop.close()


def test_dcc_connection_connect_error():
    async def failing_create_connection(*args, **kwargs):
        raise OSError('connection refused')

    loop = asyncio.new_event_loop()
    loop.create_connection = failing_create_connection
    reactor = client_aio.AioReactor(loop=loop)
    dcc = reactor.dcc()

    import irc.client

    with pytest.raises(irc.client.DCCConnectionError):
        loop.run_until_complete(dcc.connect('127.0.0.1', 12345))

    loop.close()
