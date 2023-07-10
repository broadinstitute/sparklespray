from sparklespray.csv_utils import read_csv_as_dicts
import os
from collections import OrderedDict


def test_read_csv_as_dicts():
    data_dir = os.path.join(os.path.dirname(__file__), "csv_utils_test_data")

    expected = [
        OrderedDict([("alpha", "1"), ("beta", "2")]),
        OrderedDict([("alpha", "3"), ("beta", "4")]),
    ]

    l = read_csv_as_dicts(data_dir + "/excel-utf8.csv")
    assert l == expected

    l = read_csv_as_dicts(data_dir + "/plain.csv")
    assert l == expected
