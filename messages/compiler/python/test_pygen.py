import pytest
import sys

sys.path.append("..")
import cmfc
from python import pygen
from python.serialize import CmfSerializeError


def gen_ast():
    """ Generate an AST for example.cmf """
    with open("../grammar.ebnf") as f:
        print("Reading ../grammar.ebnf")
        grammar = f.read()
        with open("../../example.cmf") as f2:
            print("Reading ../../example.cmf")
            cmf = f2.read()
        ast, _ = cmfc.parse(grammar, cmf)
        return ast


def gen_python_code(ast):
    """ Generate example.py from an ast """
    print("Generating example.py")
    code = pygen.translate(ast)
    return code


@pytest.fixture(scope='session')
def codegen(tmp_path_factory):
    """ Write example.py to testdir """
    testdir = tmp_path_factory.getbasetemp()
    code = gen_python_code(gen_ast())
    print(f"Writing example.py to {testdir}")
    with open(f"{testdir}/example.py", "w") as f:
        f.write(code)
    sys.path.insert(0, str(testdir))


def assert_roundtrip(msg):
    s = msg.serialize()
    d, _ = msg.deserialize(s)
    assert d == msg


def test_NewViewElement(codegen):
    import example

    msg = example.NewViewElement()
    msg.replica_id = 5
    msg.digest = b"somedigest"
    assert_roundtrip(msg)

    with pytest.raises(example.CmfSerializeError):
        msg.replica_id = "hello"  # not an int
        msg.serialize()

    with pytest.raises(example.CmfSerializeError):
        msg.replica_id = 999999  # larger than a uint16
        msg.serialize()

    with pytest.raises(example.CmfDeserializeError):
        msg.replica_id = 9
        s = msg.serialize()
        s[4] = 99
        msg.deserialize(s)


def test_Transaction(codegen):
    import example

    msg = example.Transaction()
    msg.name = "some-transaction"
    msg.actions = []
    assert_roundtrip(msg)

    msg.actions = [("a", "b"), ("c", "d")]
    msg.auth_key = b"some-key"
    assert_roundtrip(msg)

    msg.actions = [(5, "b")]
    with pytest.raises(example.CmfSerializeError):
        s = msg.serialize()


def test_Envelope(codegen):
    import example

    msg = example.Envelope()
    msg.version = 100
    msg.x = example.Transaction()
    msg.x.name = "some-transaction"
    msg.x.actions = [("alice", "bob")]
    msg.x.auth_key = b"you-are-authorized"
    assert_roundtrip(msg)

    # Ensure we can serialize a bytearray as bytes
    msg.x.auth_key = bytearray(msg.x.auth_key)
    assert_roundtrip(msg)

    msg.x = example.NewViewElement()
    msg.x.replica_id = 5
    msg.x.digest = b"somedigest"
    assert_roundtrip(msg)

    # missing inner digest
    msg.x.digest = None
    with pytest.raises(example.CmfSerializeError):
        msg.serialize()

    # Not part of oneof
    msg.x = 5
    with pytest.raises(example.CmfSerializeError):
        msg.serialize()


def test_NewStuff(codegen):
    import example

    msg = example.NewStuff()
    msg.crazy_map = dict()
    assert_roundtrip(msg)

    msg.crazy_map = {"a": []}
    assert_roundtrip(msg)

    msg.crazy_map = {"alice": [("a", "b"), ("charles", "diane")]}
    assert_roundtrip(msg)

    msg.crazy_map = {5: []}
    with pytest.raises(example.CmfSerializeError):
        msg.serialize()

    msg.crazy_map = {"b": {}}
    with pytest.raises(example.CmfSerializeError):
        msg.serialize()

    msg.crazy_map = {"b": ["x"]}
    with pytest.raises(example.CmfSerializeError):
        msg.serialize()

    msg.crazy_map = []
    with pytest.raises(example.CmfSerializeError):
        msg.serialize()


def test_WithMsgRefs(codegen):
    import example

    msg = example.WithMsgRefs()
    msg.new_stuff = example.NewStuff()
    msg.new_stuff.crazy_map = {}
    msg.tx_list = []
    msg.map_of_envelope = {}
    assert_roundtrip(msg)

    tx1 = example.Transaction()
    tx1.name = "tx1"
    tx1.actions = []
    tx1.auth_key = b'key'

    tx2 = example.Transaction()
    tx2.name = "tx2"
    tx2.actions = [("a", "b")]
    msg.tx_list = [tx1, tx2]

    assert_roundtrip(msg)

    envelope = example.Envelope()
    envelope.version = 1
    envelope.x = tx1
    msg.map_of_envelope = {"env1": envelope}
    assert_roundtrip(msg)

    # Transaction instead of envelope
    msg.map_of_envelope = {"env1": tx1}
    with pytest.raises(example.CmfSerializeError):
        msg.serialize()

    # Try to deserialize a bad buffer
    msg.map_of_envelope = {"env1": envelope}
    s = msg.serialize()
    s = s[0:len(s) - 1]
    with pytest.raises(example.CmfDeserializeError):
        msg.deserialize(s)


def test_FixedBuffer(codegen):
    import example

    msg = example.FixedBuffer()
    msg.bytes8 = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    msg.bytes4 = [0x0a, 0x0b, 0x0c, 0x0d]

    assert_roundtrip(msg)

    msg.bytes8 = [0x01, 0x02, 0x03, 0x04]
    with pytest.raises(example.CmfSerializeError):
        s = msg.serialize()

    msg.bytes4 = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    with pytest.raises(example.CmfSerializeError):
        s = msg.serialize()


def test_FixedTransactionList(codegen):
    import example

    t1 = example.Transaction()
    t1.name = "transaction1"
    t1.actions = []

    t2 = example.Transaction()
    t2.name = "transaction2"
    t2.actions = []

    t3 = example.Transaction()
    t3.name = "transaction3"
    t3.actions = []

    msg = example.FixedTransactionList()
    msg.transactions1 = [t1]
    msg.transactions2 = [t2, t3]

    assert_roundtrip(msg)

    msg.transactions1 = [t2, t3]
    with pytest.raises(example.CmfSerializeError):
        s = msg.serialize()

    msg.transactions2 = [t1]
    with pytest.raises(example.CmfSerializeError):
        s = msg.serialize()

def test_ContainsEnum(codegen):
    import example

    msg = example.ContainsEnum()
    msg.color = example.Color(1)
    msg.colors = [example.Color(0)]
    msg.colors_by_name = {'red': example.Color(0)}
    assert_roundtrip(msg)

    # Try to serialize an invalid type
    msg.color = 'red'
    with pytest.raises(example.CmfSerializeError):
        msg.serialize()

    # Try to serialize a class with the same name but that isn't an enum
    class Color:
        pass
    msg.color = Color()
    with pytest.raises(example.CmfSerializeError):
        msg.serialize()

    # Try to deserialize a message with a value outside the enum range (0-2)
    s = bytearray(b'\x03')
    with pytest.raises(ValueError):
        msg.deserialize(s)
