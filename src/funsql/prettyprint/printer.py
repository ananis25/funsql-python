"""
This module implements a `Printer` object to print the FunSQL query 
structure, since they can get long and unwieldy. Adapted from:
* https://github.com/dtolnay/prettyplease/
* https://github.com/stevej2608/oppen-pretty-printer

both of which implement the algorithm first implemented by Derek C. Oppen. 
"""


import io
from enum import Enum
from collections import deque
from typing import Union, NamedTuple


SIZE_INFINITY = 1000_000

# -----------------------------------------------------------
# primitives to assmeble an expression to print
# -----------------------------------------------------------

Token = Union[str, "Break", "Begin", "End"]


class GroupBreak(Enum):
    fits = 1
    consistent = 2
    inconsistent = 3


class Break:
    blanks: int
    offset: int

    def __init__(self, blanks: int = 0, offset: int = 0) -> None:
        self.blanks = blanks
        self.offset = offset


class Begin:
    """
    Setting `ns_indent` to True overrides the offset for that `Begin...End`
    block to the cursor position at start. So, we can get formatting like:
    ```
    begin
         x = 1
    end
    ```
    instead of the regular (assuming, offset = 2):
    ```
    begin
      x = 1
    end
    ```
    with standard 2 or 4 space indentation.
    """

    offset: int
    break_typ: GroupBreak
    ns_indent: bool  # non-standard indent

    def __init__(
        self,
        offset: int = 0,
        break_typ: GroupBreak = GroupBreak.consistent,
        ns_indent: bool = False,
    ) -> None:
        self.offset = offset
        self.break_typ = break_typ
        self.ns_indent = ns_indent


class End:
    pass


# -----------------------------------------------------------
# The `Printer` object which does the work
# -----------------------------------------------------------


class PrintFrame(NamedTuple):
    break_typ: GroupBreak
    offset: int = 0


class TokenEntry:
    token: Token
    size: int

    def __init__(self, tok: Token, size: int) -> None:
        self.token = tok
        self.size = size


class Printer:
    _buf_offset: int  # incremented on each `advance_left` operation
    buffer: deque[TokenEntry]  # ringbuffer of tokens along with their calculated sizes
    left_total: int  # sum of size of tokens already printed
    right_total: int  # sum of size of tokens printed or queued
    scan_stack: deque[int]  # tracks the index of delimiter tokens in the buffer
    writer: "TokenWriter"

    def __init__(self, line_width: int = 80) -> None:
        self._buf_offset = 0
        self.buffer = deque()
        self.left_total = 0
        self.right_total = 0
        self.scan_stack = deque()
        self.writer = TokenWriter(line_width)

    @property
    def buffer_idx_last(self) -> int:
        return self._buf_offset + len(self.buffer) - 1

    @property
    def buffer_idx_first(self) -> int:
        return self._buf_offset

    def scan(self, token: Token):
        if isinstance(token, str):
            self.scan_str(token)
        elif isinstance(token, Begin):
            self.scan_begin(token)
        elif isinstance(token, End):
            self.scan_end(token)
        elif isinstance(token, Break):
            self.scan_break(token)
        else:
            raise ValueError(f"unexpected token type: {type(token)}")

    def scan_begin(self, token: Begin):
        if len(self.scan_stack) == 0:
            self.left_total = 1
            self.right_total = 1
            self.buffer.clear()

        entry = TokenEntry(token, -self.right_total)
        self.buffer.append(entry)
        self.scan_stack.append(self.buffer_idx_last)

    def scan_end(self, token: End):
        if len(self.scan_stack) == 0:
            self.writer.print(token, 0)
        else:
            entry = TokenEntry(token, -1)
            self.buffer.append(entry)
            self.scan_stack.append(self.buffer_idx_last)

    def scan_break(self, token: Break):
        if len(self.scan_stack) == 0:
            self.left_total = 1
            self.right_total = 1
            self.buffer.clear()

        self.check_stack(0)
        entry = TokenEntry(token, -self.right_total)
        self.buffer.append(entry)
        self.scan_stack.append(self.buffer_idx_last)
        self.right_total += token.blanks

    def scan_str(self, token: str):
        size = len(token)
        if len(self.scan_stack) == 0:
            self.writer.print(token, size)
        else:
            entry = TokenEntry(token, size)
            self.buffer.append(entry)
            self.right_total += size
            self.check_stream()

    def eof(self) -> None:
        """Resolve any outstanding tokens in the buffer, and push them to the writer"""
        if len(self.scan_stack) > 0:
            self.check_stack(0)
            self.advance_left()

    def getvalue(self) -> str:
        return self.writer.getvalue()

    def advance_left(self):
        while self.buffer[0].size >= 0:
            entry = self.buffer.popleft()
            self._buf_offset += 1

            self.writer.print(entry.token, entry.size)
            if isinstance(entry.token, str):
                assert len(entry.token) == entry.size
                self.left_total += entry.size
            elif isinstance(entry.token, Break):
                self.left_total += entry.token.blanks

            if len(self.buffer) == 0:
                break

    def check_stack(self, depth: int):
        """Takes elements off the scan stack if their sizes are known.
        This method gets called on getting a `Break` token, or at the end.
        """
        assert depth >= 0
        while len(self.scan_stack) > 0:
            idx = self.scan_stack[-1]
            entry = self.buffer[idx - self.buffer_idx_first]

            if isinstance(entry.token, Begin):
                # Begin -> ..., nothing to do here
                if depth == 0:
                    break
                else:
                    # Begin -> (End) -> ..., so we can resolve the Begin token
                    self.scan_stack.pop()
                    entry.size += self.right_total
                    depth -= 1
            elif isinstance(entry.token, End):
                # End -> ..., recurse to resolve the Begin token
                self.scan_stack.pop()
                entry.size = 1
                depth += 1
            elif isinstance(entry.token, Break):
                # Break -> ..., resolve the previous Break token
                self.scan_stack.pop()
                entry.size += self.right_total
                if depth == 0:
                    break
            else:
                raise Exception("unreachable case")

    def check_stream(self):
        """Check if the current line has gone beyond the available width"""
        while self.right_total - self.left_total > self.writer.space:
            if len(self.scan_stack) > 0:
                if self.buffer_idx_first == self.scan_stack[0]:
                    self.scan_stack.popleft()
                    self.buffer[0].size = SIZE_INFINITY

            self.advance_left()
            if len(self.buffer) == 0:
                break


class TokenWriter:
    output: io.StringIO
    print_stack: list[PrintFrame]
    line_width: int
    space: int
    indent: int

    def __init__(self, line_width) -> None:
        self.output = io.StringIO()
        self.print_stack = []
        self.line_width = line_width
        self.space = line_width
        self.indent = 0

    def print(self, tok: Token, size: int):
        """Called by the Scanner object to an output buffer"""
        if isinstance(tok, str):
            self.output.write(tok)
            self.space -= size

        elif isinstance(tok, Begin):
            if size > self.space:
                # record the current indentation for recovery later
                self.print_stack.append(PrintFrame(tok.break_typ, self.indent))
                if tok.ns_indent:
                    self.indent = self.line_width - self.space
                else:
                    self.indent = self.indent + tok.offset
            else:
                self.print_stack.append(PrintFrame(GroupBreak.fits, 0))

        elif isinstance(tok, End):
            break_typ, indent = self.print_stack.pop()
            if break_typ != GroupBreak.fits:
                self.indent = indent  # restore the original indent level

        elif isinstance(tok, Break):
            top = self.print_stack[-1]
            fits_line = top.break_typ == GroupBreak.fits or (
                top.break_typ == GroupBreak.inconsistent and size <= self.space
            )
            if fits_line:
                self.space -= tok.blanks
                self.output.write(" " * tok.blanks)
            else:
                self.output.write("\n")
                offset = self.indent + tok.offset
                self.output.write(" " * offset)
                self.space = self.line_width - offset

        else:
            raise ValueError(f"Unknown token type: {type(tok)}")

    def getvalue(self) -> str:
        self.output.flush()
        return self.output.getvalue()
