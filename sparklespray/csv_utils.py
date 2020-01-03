import csv
import io
import encodings
import codecs

# from codecs import BOM_UTF8, BOM_UTF16_BE, BOM_UTF16_LE, BOM_UTF32_BE, BOM_UTF32_LE
#
# BOMS = (
#     (BOM_UTF8, "UTF-8"),
#     (BOM_UTF32_BE, "UTF-32-BE"),
#     (BOM_UTF32_LE, "UTF-32-LE"),
#     (BOM_UTF16_BE, "UTF-16-BE"),
#     (BOM_UTF16_LE, "UTF-16-LE"),
# )
# # sourced from https://unicodebook.readthedocs.io/guess_encoding.html
# def check_bom(data):
#     return [encoding for bom, encoding in BOMS if data.startswith(bom)]
#
# def read_csv_as_dicts(filename):
#     with open(filename, "rb") as fd:
#         contents = fd.read(100)
#         io.BufferedReader
#
#     with open(filename, "rt") as fd:
#         return list(csv.DictReader(fd))

def read_csv_as_dicts(filename):
    # Check for BOM in case csv was written by excel
    with open(filename, "rb") as fd:
        possible_bom = fd.read(len(codecs.BOM_UTF8))
        if possible_bom == codecs.BOM_UTF8:
            encoding = 'utf-8-sig'
        else:
            encoding = 'ascii'

    with open(filename, "rt", encoding=encoding) as fd:
        return list(csv.DictReader(fd))