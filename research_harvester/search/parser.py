"""Boolean query parser: fields, parentheses, phrases, AND/OR/NOT.

Grammar (keywords AND/OR/NOT are ASCII uppercase only)
------------------------------------------------------
    or_expr   := and_expr (OR and_expr)*
    and_expr  := factor ((AND? factor) | (AND? NOT factor))*
    factor    := unary (NOT unary)*
    unary     := primary                    # a leading NOT is rejected:
                                            # NOT is binary difference only
    primary   := '(' or_expr ')' | leaf
    leaf      := [field ':'] (PHRASE | WORD)

Rules pinned here
-----------------
* Adjacent values with no operator imply AND (default AND).
* NOT is a *binary* set difference ``A NOT B`` (also accepted as the
  ``A AND NOT B`` idiom).  A leading NOT or ``OR NOT`` has no left operand and
  is a syntax error.
* Field names are exactly ``title`` and ``abstract`` (lowercase); anything
  else is an unknown-field error at the field's position.
* Empty parentheses, empty phrases, dangling operators and unclosed quotes are
  errors with a character position; parse failure NEVER degrades into
  plain-text search and the expression is never concatenated into SQL.
* An unquoted word that tokenizes to several tokens (e.g. CJK ``蛋白``) means
  implicit AND of those tokens; only a quoted phrase requires consecutive
  token positions.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from . import FIELDS
from .tokenizer import term_terms

KEYWORDS = {"AND", "OR", "NOT"}


class QuerySyntaxError(Exception):
    """Syntax error carrying a 0-based position in the original expression."""

    def __init__(self, message: str, position: int, expression: str):
        self.message = message
        self.position = position          # 0-based char offset
        self.expression = expression
        pointer = " " * min(position, max(0, len(expression))) + "^"
        super().__init__(
            f"语法错误（位置 {position + 1}）：{message}\n{expression}\n{pointer}")


@dataclass
class Node:
    start: int
    end: int


@dataclass
class TermNode(Node):
    field: Optional[str]                 # None -> either field
    phrase: bool
    raw: str                             # raw surface text (unquoted)
    terms: List[str] = field(default_factory=list)


@dataclass
class AndNode(Node):
    children: List[Node]


@dataclass
class OrNode(Node):
    children: List[Node]


@dataclass
class DiffNode(Node):
    left: Node
    right: Node


@dataclass
class _Token:
    kind: str                            # WORD|PHRASE|LPAREN|RPAREN|COLON|AND|OR|NOT
    text: str
    start: int
    end: int


_SPECIAL = set('():"')


def _lex(expression: str) -> List[_Token]:
    tokens: List[_Token] = []
    i = 0
    n = len(expression)
    while i < n:
        ch = expression[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "(":
            tokens.append(_Token("LPAREN", ch, i, i + 1))
            i += 1
            continue
        if ch == ")":
            tokens.append(_Token("RPAREN", ch, i, i + 1))
            i += 1
            continue
        if ch == ":":
            tokens.append(_Token("COLON", ch, i, i + 1))
            i += 1
            continue
        if ch == '"':
            quote_start = i
            j = i + 1
            buf = []
            while j < n and expression[j] != '"':
                buf.append(expression[j])
                j += 1
            if j >= n:
                raise QuerySyntaxError("引号没有闭合，短语缺少结束的 \"",
                                       quote_start, expression)
            tokens.append(_Token("PHRASE", "".join(buf), quote_start, j + 1))
            i = j + 1
            continue
        start = i
        buf = []
        while i < n and not expression[i].isspace() \
                and expression[i] not in _SPECIAL:
            buf.append(expression[i])
            i += 1
        word = "".join(buf)
        if word in KEYWORDS:
            tokens.append(_Token(word, word, start, i))
        else:
            tokens.append(_Token("WORD", word, start, i))
    return tokens


class _Parser:
    def __init__(self, expression: str):
        self.expr = expression
        self.tokens = _lex(expression)
        self.pos = 0

    def peek(self) -> Optional[_Token]:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def take(self) -> Optional[_Token]:
        tok = self.peek()
        if tok is not None:
            self.pos += 1
        return tok

    def error(self, message: str, position: int) -> QuerySyntaxError:
        return QuerySyntaxError(message, position, self.expr)

    def parse(self) -> Node:
        if not self.tokens:
            raise self.error("表达式为空", 0)
        node = self.parse_or()
        leftover = self.peek()
        if leftover is not None:
            if leftover.kind == "RPAREN":
                raise self.error("多余的右括号 ')'", leftover.start)
            raise self.error(f"无法识别的片段 {leftover.text!r}",
                             leftover.start)
        return node

    def parse_or(self) -> Node:
        left = self.parse_and()
        while self.peek() is not None and self.peek().kind == "OR":
            tok = self.take()
            nxt = self.peek()
            self._require_operand(tok, nxt, "OR")
            if nxt.kind == "NOT":
                raise self.error(
                    "NOT 只能作二元差集，OR 后的 NOT 缺少左操作数", nxt.start)
            right = self.parse_and()
            if isinstance(left, OrNode):
                left.children.append(right)
                left.end = right.end
            else:
                left = OrNode(left.start, right.end, [left, right])
        return left

    def parse_and(self) -> Node:
        left = self.parse_factor()
        while True:
            tok = self.peek()
            if tok is None or tok.kind in ("OR", "RPAREN"):
                return left
            if tok.kind == "AND":
                self.take()
                nxt = self.peek()
                self._require_operand(tok, nxt, "AND")
                if nxt.kind == "NOT":
                    # A AND NOT B  ==  A NOT B
                    self.take()
                    after = self.peek()
                    self._require_operand(nxt, after, "NOT")
                    right = self.parse_factor()
                    left = DiffNode(left.start, right.end, left, right)
                    continue
                right = self.parse_factor()
                left = self._merge_and(left, right)
                continue
            if tok.kind == "NOT":
                self.take()
                nxt = self.peek()
                self._require_operand(tok, nxt, "NOT")
                right = self.parse_factor()
                left = DiffNode(left.start, right.end, left, right)
                continue
            if tok.kind == "COLON":
                raise self.error("冒号 ':' 前缺少字段名", tok.start)
            # WORD / PHRASE / LPAREN -> implicit AND
            right = self.parse_factor()
            left = self._merge_and(left, right)

    @staticmethod
    def _merge_and(left: Node, right: Node) -> Node:
        if isinstance(left, AndNode) and not isinstance(right, AndNode):
            left.children.append(right)
            left.end = right.end
            return left
        if isinstance(left, AndNode) and isinstance(right, AndNode):
            left.children.extend(right.children)
            left.end = right.end
            return left
        return AndNode(left.start, right.end, [left, right])

    def _require_operand(self, op_tok, nxt, op_name):
        if nxt is None:
            raise self.error(f"残缺的运算符 {op_name}：右侧缺少表达式",
                             op_tok.start)
        if nxt.kind in ("AND", "OR", "RPAREN"):
            raise self.error(
                f"残缺的运算符 {op_name}：右侧意外遇到 {nxt.kind}", nxt.start)
        if nxt.kind == "COLON":
            raise self.error("冒号 ':' 前缺少字段名", nxt.start)

    def parse_factor(self) -> Node:
        tok = self.peek()
        if tok is None:
            raise self.error("表达式意外结束", len(self.expr))
        if tok.kind == "NOT":
            raise self.error(
                "NOT 只能作二元差集，不能作为前缀（缺少左操作数）", tok.start)
        if tok.kind in ("AND", "OR"):
            raise self.error(f"残缺的运算符 {tok.kind}：左侧缺少表达式",
                             tok.start)
        if tok.kind == "RPAREN":
            raise self.error("多余的右括号 ')'", tok.start)
        if tok.kind == "COLON":
            raise self.error("冒号 ':' 前缺少字段名", tok.start)
        left = self.parse_primary()
        while self.peek() is not None and self.peek().kind == "NOT":
            tok = self.take()
            nxt = self.peek()
            self._require_operand(tok, nxt, "NOT")
            right = self.parse_primary()
            left = DiffNode(left.start, right.end, left, right)
        return left

    def parse_primary(self) -> Node:
        tok = self.peek()
        if tok is None:
            raise self.error("表达式意外结束", len(self.expr))
        if tok.kind == "LPAREN":
            open_tok = self.take()
            nxt = self.peek()
            if nxt is not None and nxt.kind == "RPAREN":
                raise self.error("空括号 ()：括号内必须有表达式",
                                 open_tok.start)
            inner = self.parse_or()
            close = self.peek()
            if close is None:
                raise self.error("括号没有闭合，缺少 ')'", open_tok.start)
            if close.kind != "RPAREN":
                raise self.error(f"括号内表达式残缺，意外遇到 {close.kind}",
                                 close.start)
            self.take()
            inner.start = open_tok.start
            inner.end = close.end
            return inner
        return self.parse_leaf()

    def parse_leaf(self) -> Node:
        tok = self.take()
        if tok is None:
            raise self.error("表达式意外结束", len(self.expr))
        if tok.kind == "PHRASE":
            return self._make_term(None, True, tok.text, tok.start, tok.end)
        if tok.kind != "WORD":
            raise self.error(f"此处应是检索词或短语，却遇到 {tok.kind}",
                             tok.start)
        word, start = tok.text, tok.start
        nxt = self.peek()
        if nxt is not None and nxt.kind == "COLON":
            if word not in FIELDS:
                raise self.error(
                    f"未知字段 {word!r}：支持的字段只有 "
                    f"{'、'.join(FIELDS)}", start)
            colon = self.take()
            value = self.peek()
            if value is None:
                raise self.error(f"字段 {word!r} 的冒号后缺少检索词",
                                 colon.start)
            if value.kind in ("AND", "OR", "NOT", "RPAREN", "LPAREN",
                              "COLON"):
                raise self.error(f"字段 {word!r} 的冒号后缺少检索词",
                                 colon.start)
            value_tok = self.take()
            return self._make_term(word, value_tok.kind == "PHRASE",
                                   value_tok.text, start, value_tok.end)
        return self._make_term(None, False, word, start, tok.end)

    def _make_term(self, field, is_phrase, raw, start, end) -> Node:
        terms = term_terms(raw)
        if not terms:
            if is_phrase:
                raise self.error(
                    "空短语：引号内必须包含至少一个可检索词项", start)
            raise self.error(
                "检索词在规范化后没有任何可检索词项（只含标点/空白）", start)
        if is_phrase or len(terms) == 1:
            return TermNode(start, end, field, is_phrase, raw, terms)
        # unquoted multi-token word (e.g. CJK): implicit AND of its tokens
        return AndNode(start, end,
                       [TermNode(start, end, field, False, raw, [t])
                        for t in terms])


def parse(expression: str) -> Node:
    """Parse a query expression into an AST or raise QuerySyntaxError."""
    if not isinstance(expression, str):
        raise QuerySyntaxError("表达式必须是字符串", 0, "")
    return _Parser(expression).parse()


def walk(node: Node):
    """Yield every node in the AST (pre-order)."""
    yield node
    if isinstance(node, (AndNode, OrNode)):
        for child in node.children:
            yield from walk(child)
    elif isinstance(node, DiffNode):
        yield from walk(node.left)
        yield from walk(node.right)


def leaves(node: Node) -> List[TermNode]:
    return [n for n in walk(node) if isinstance(n, TermNode)]
