from os import path

from ....utils.ciff.to_csv import ToCSV


def test_create_csv_from_ciff(tmp_path):
    ToCSV(protobuf_file=path.dirname(path.dirname(path.dirname(__file__)))+'/resources/ciff/sample-core18-postings.pb',
          docs_file=path.dirname(path.dirname(path.dirname(__file__)))+'/resources/ciff/sample-core18-docs.txt',
          output_docs=str(tmp_path) + 'docs.csv',
          output_term_dict=str(tmp_path) + 'term_dict.csv',
          output_term_doc=str(tmp_path) + 'term_doc.csv'
          )
    with open(str(tmp_path) + 'docs.csv') as f:
        assert f.readline().strip() == 'b2e89334-33f9-11e1-825f-dabc29fd7071|0|600'
    with open(str(tmp_path) + 'term_dict.csv') as f:
        assert f.readline().strip() == '0|236|0'
    with open(str(tmp_path) + 'term_doc.csv') as f:
        assert f.readline().strip() == '0|3|3'
