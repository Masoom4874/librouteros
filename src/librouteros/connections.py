# -*- coding: UTF-8 -*-

import socket
from asyncio import StreamReader, StreamWriter
from contextlib import suppress

from librouteros.exceptions import ConnectionClosed, RouterSyncTimeoutError

ABANDONED_MESSAGE = "Connection abandoned after a router timeout; open a new one."


class SocketTransport:
    def __init__(self, sock: socket.socket) -> None:
        self.sock: socket.socket = sock
        self.abandoned: bool = False

    def write(self, data: bytes) -> None:
        """
        Write given bytes to socket. Loop as long as every byte in
        string is written unless exception is raised.
        """
        if self.abandoned:
            raise ConnectionClosed(ABANDONED_MESSAGE)
        try:
            self.sock.sendall(data)
        except socket.timeout as error:
            self.abandon()
            raise RouterSyncTimeoutError from error

    def read(self, length: int) -> bytes:
        """
        Read as many bytes from socket as specified in length.
        Loop as long as every byte is read unless exception is raised.
        """
        if self.abandoned:
            raise ConnectionClosed(ABANDONED_MESSAGE)
        data: bytearray = bytearray()
        try:
            while (to_read := length - len(data)) != 0:
                got: bytes = self.sock.recv(to_read)
                if not got:
                    raise ConnectionClosed("Connection unexpectedly closed.")
                data += got
        except socket.timeout as error:
            self.abandon()
            raise RouterSyncTimeoutError from error
        return bytes(data)

    def abandon(self) -> None:
        """
        Drop the socket after a timeout.

        The router may still answer the timed-out request later. Nothing may
        read that late reply as the answer to the next request, so the socket
        is closed and any further use raises ConnectionClosed.
        """
        self.abandoned = True
        with suppress(OSError):
            self.sock.close()

    def close(self) -> None:
        if self.abandoned:
            return
        self.sock.close()


class AsyncSocketTransport:
    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.reader: StreamReader = reader
        self.writer: StreamWriter = writer
        self.abandoned: bool = False

    async def write(self, data: bytes) -> None:
        """
        Write given bytes to socket. Loop as long as every byte in
        string is written unless exception is raised.
        """
        if self.abandoned:
            raise ConnectionClosed(ABANDONED_MESSAGE)
        self.writer.write(data)
        await self.writer.drain()

    async def read(self, length: int) -> bytes:
        """
        Read as many bytes from socket as specified in length.
        Loop as long as every byte is read unless exception is raised.
        """
        if self.abandoned:
            raise ConnectionClosed(ABANDONED_MESSAGE)
        data: bytearray = bytearray()
        while (to_read := length - len(data)) != 0:
            got: bytes = await self.reader.read(to_read)
            if not got:
                raise ConnectionClosed("Connection unexpectedly closed.")
            data += got
        return bytes(data)

    def abandon(self) -> None:
        """
        Drop the socket after a timeout.

        The router may still answer the timed-out request later. Nothing may
        read that late reply as the answer to the next request, so the socket
        is closed and any further use raises ConnectionClosed. Synchronous on
        purpose: it runs inside a timeout handler, and waiting for the peer to
        acknowledge the close could hang on the same dead link.
        """
        self.abandoned = True
        with suppress(OSError):
            self.writer.close()

    async def close(self) -> None:
        if self.abandoned:
            # Already closed by abandon(). Waiting for wait_closed() here could
            # block until TCP gives up if the timed-out write left unsent bytes.
            return
        self.writer.close()
        await self.writer.wait_closed()
