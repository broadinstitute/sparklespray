from .list import process_records, write_csv
import io


def test_process_records():
    records = [
        dict(name="puma", type="kitty cat", age="100"),
        dict(name="sir-quackers", type="duck", age="101"),
        dict(name="john", type="moose", age="102"),
    ]

    # test use of fields and no filters
    filtered = process_records(records, fields=["name", "age"], filter_expressions=[])
    assert len(filtered) == 3
    assert filtered[1] == {"name": "sir-quackers", "age": "101"}

    # test use of filters and getting all fields
    filtered = process_records(records, fields=None, filter_expressions=["type=duck"])
    assert filtered == [records[1]]

    filtered = process_records(records, fields=None, filter_expressions=["type!=duck"])
    assert filtered == [records[0], records[2]]


def test_process_nested_records():
    records = [dict(a=dict(b="1", c="2"), d="3"), dict(a=dict(b="4", c="5"), d="6")]

    # test use of fields and no filters
    filtered = process_records(records, fields=["a.b"], filter_expressions=[])
    assert len(filtered) == 2
    assert filtered == [{"a": {"b": "1"}}, {"a": {"b": "4"}}]

    # test use of filters and getting all fields
    filtered = process_records(records, fields=None, filter_expressions=["a.b=4"])
    assert filtered == [records[1]]


def test_write_csv():
    # test flattening of nested structures when writing csv
    f = io.StringIO()
    write_csv([{"a": {"b": "c1", "d": "e2"}}, {"a": {"d": "e2"}, "b": "c2"}], f)
    assert f.getvalue().replace("\r", "") == "a.b,a.d,b\nc1,e2,\n,e2,c2\n"
