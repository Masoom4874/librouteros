# -*- coding: UTF-8 -*-

import socket
from asyncio import StreamReader, StreamWriter
from unittest.mock import MagicMock, call

import pytest

from librouteros.connections import AsyncSocketTransport, SocketTransport
from librouteros.exceptions import (
    ConnectionClosed,
    RouterSyncTimeoutError,
)


class Test_SocketTransport:
    def setup_method(self):
        self.transport = SocketTransport(sock=MagicMock(spec=socket.socket))

    def test_close_shutdown_exception(self):
        self.transport.sock.shutdown.side_effect = socket.error
        self.transport.close()
        self.transport.sock.close.assert_called_once_with()

    def test_close(self):
        self.transport.close()
        self.transport.sock.close.assert_called_once_with()

    def test_calls_sendall(self):
        self.transport.write(b"some message")
        self.transport.sock.sendall.assert_called_once_with(b"some message")

    def test_read_raises_when_recv_returns_empty_byte_string(self):
        self.transport.sock.recv.return_value = b""
        with pytest.raises(ConnectionClosed):
            self.transport.read(3)
        assert self.transport.sock.recv.call_args_list == [
            call(3),
        ]

    def test_read_raises_when_broken_stream(self):
        # Simulate broken stream after receiving 5 bytes
        self.transport.sock.recv.side_effect = (b"valid", b"")
        with pytest.raises(ConnectionClosed):
            self.transport.read(8)
        assert self.transport.sock.recv.call_args_list == [
            call(8),
            call(3),
        ]

    def test_read_reads_full_length(self):
        # Simulate fragmented stream of bytes
        self.transport.sock.recv.side_effect = (b"retu", b"rne", b"d", b"other_data")
        assert self.transport.read(8) == b"returned"
        assert self.transport.sock.recv.call_args_list == [
            call(8),
            call(4),
            call(1),
        ]

    @pytest.mark.parametrize("exception", (socket.error,))
    def test_recv_raises_socket_errors(self, exception):
        """Non-timeout socket errors propagate unchanged."""
        self.transport.sock.recv.side_effect = exception
        with pytest.raises(exception):
            self.transport.read(2)

    def test_recv_timeout_raises_RouterSyncTimeoutError(self):
        """A socket recv timeout is converted to RouterSyncTimeoutError (and chained)."""
        self.transport.sock.recv.side_effect = socket.timeout
        with pytest.raises(RouterSyncTimeoutError) as error:
            self.transport.read(2)
        assert isinstance(error.value, OSError)
        assert isinstance(error.value.__cause__, socket.timeout)

    def test_recv_timeout_abandons_socket(self):
        """After a recv timeout the socket is closed and the transport refuses further use."""
        self.transport.sock.recv.side_effect = socket.timeout("timed out")
        with pytest.raises(RouterSyncTimeoutError):
            self.transport.read(1)
        self.transport.sock.close.assert_called_once_with()
        assert self.transport.abandoned is True
        with pytest.raises(ConnectionClosed, match="abandoned"):
            self.transport.read(1)
        with pytest.raises(ConnectionClosed, match="abandoned"):
            self.transport.write(b"x")
        self.transport.sock.recv.assert_called_once()
        self.transport.sock.sendall.assert_not_called()

    def test_sendall_timeout_abandons_socket(self):
        """After a sendall timeout the socket is closed and the transport refuses further use."""
        self.transport.sock.sendall.side_effect = socket.timeout("timed out")
        with pytest.raises(RouterSyncTimeoutError):
            self.transport.write(b"x")
        self.transport.sock.close.assert_called_once_with()
        assert self.transport.abandoned is True
        with pytest.raises(ConnectionClosed, match="abandoned"):
            self.transport.write(b"x")

    def test_sendall_timeout_raises_RouterSyncTimeoutError(self):
        """A socket sendall timeout is converted to RouterSyncTimeoutError (and chained)."""
        self.transport.sock.sendall.side_effect = socket.timeout
        with pytest.raises(RouterSyncTimeoutError) as error:
            self.transport.write(b"data")
        assert isinstance(error.value, OSError)
        assert isinstance(error.value.__cause__, socket.timeout)


class Test_AsyncSocketTransport:
    def setup_method(self):
        self.transport = AsyncSocketTransport(
            reader=MagicMock(spec=StreamReader),
            writer=MagicMock(spec=StreamWriter),
        )

    @pytest.mark.asyncio
    async def test_close(self):
        await self.transport.close()
        self.transport.writer.close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_write(self):
        await self.transport.write(b"some message")
        self.transport.writer.write.assert_called_once_with(b"some message")
        self.transport.writer.drain.assert_called_once_with()

    def test_abandon_closes_writer_without_waiting(self):
        """abandon() runs inside a timeout handler, so it must not await the peer."""
        self.transport.abandon()
        self.transport.writer.close.assert_called_once_with()
        self.transport.writer.wait_closed.assert_not_called()
        assert self.transport.abandoned is True

    def test_abandon_swallows_close_errors(self):
        self.transport.writer.close.side_effect = OSError("already closing")
        self.transport.abandon()
        assert self.transport.abandoned is True

    @pytest.mark.asyncio
    async def test_write_after_abandon_raises_ConnectionClosed(self):
        self.transport.abandon()
        with pytest.raises(ConnectionClosed, match="abandoned"):
            await self.transport.write(b"some message")
        self.transport.writer.write.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_after_abandon_raises_ConnectionClosed(self):
        self.transport.abandon()
        with pytest.raises(ConnectionClosed, match="abandoned"):
            await self.transport.read(1)
        self.transport.reader.read.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_raises_when_recv_returns_empty_byte_string(self):
        self.transport.reader.read.return_value = b""
        with pytest.raises(ConnectionClosed):
            await self.transport.read(3)
        assert self.transport.reader.read.call_args_list == [
            call(3),
        ]

    @pytest.mark.asyncio
    async def test_read_raises_when_broken_stream(self):
        # Simulate broken stream after receiving 5 bytes
        self.transport.reader.read.side_effect = (b"valid", b"")
        with pytest.raises(ConnectionClosed):
            await self.transport.read(8)
        assert self.transport.reader.read.call_args_list == [
            call(8),
            call(3),
        ]

    @pytest.mark.asyncio
    async def test_read_reads_full_length(self):
        # Simulate fragmented stream of bytes
        self.transport.reader.read.side_effect = (b"retu", b"rne", b"d", b"other_data")
        assert await self.transport.read(8) == b"returned"
        assert self.transport.reader.read.call_args_list == [
            call(8),
            call(4),
            call(1),
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exception", (socket.error, socket.timeout))
    async def test_recv_raises_socket_errors(self, exception):
        self.transport.reader.read.side_effect = exception
        with pytest.raises(exception):
            await self.transport.read(2)
