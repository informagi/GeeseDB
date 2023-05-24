from pathlib import Path
from typing import IO, Iterable, Optional, Union

from google.protobuf.internal.encoder import _VarintEncoder  # type: ignore
from google.protobuf.message import Message

from ..ciff.CommonIndexFileFormat_pb2 import DocRecord, Header, PostingsList


class MessageWriter:
    filename: Optional[Path] = None
    output: Optional[IO[bytes]] = None

    def __init__(self, output: Union[Path, IO[bytes]]) -> None:
        if isinstance(output, Path):
            self.filename = output
        else:
            self.output = output

        self.varint_encoder = _VarintEncoder()

    def write_message(self, message: Message):
        self.write_serialized(message.SerializeToString())

    def write_serialized(self, serialized_message: bytes):
        if self.output is None:
            raise ValueError('cannot write to closed file')

        self.varint_encoder(self.output.write, len(serialized_message))
        self.output.write(serialized_message)

    def write(self, data: bytes):
        if self.output is None:
            raise ValueError('cannot write to closed file')

        self.output.write(data)

    def __enter__(self):
        if self.filename is not None:
            self.output = open(self.filename, 'wb')

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.filename is not None:
            self.output.close()


class CiffWriter(MessageWriter):
    def write_header(self, header: Header):
        self.write_message(header)

    def write_documents(self, documents: Iterable[DocRecord]):
        for doc in documents:
            self.write_message(doc)

    def write_postings_lists(self, postings_lists: Iterable[PostingsList]):
        for pl in postings_lists:
            self.write_message(pl)