from os import path

from ....utils.ciff.to_csv import ToCSV


def test_create_csv_from_ciff(tmp_path: str) -> None:
    ToCSV(
        protobuf_file=path.dirname(path.dirname(path.dirname(__file__))) + '/resources/ciff/toy-complete-20200309.ciff.gz',
        output_docs=str(tmp_path) + 'docs.csv',
        output_term_dict=str(tmp_path) + 'term_dict.csv',
        output_term_doc=str(tmp_path) + 'term_doc.csv'
        )
    with open(str(tmp_path) + 'docs.csv') as f:
        assert f.readline().strip() == 'WSJ_1|0|6'
    with open(str(tmp_path) + 'term_dict.csv') as f:
        assert f.readline().strip() == '0|1|01'
    with open(str(tmp_path) + 'term_doc.csv') as f:
        assert f.readline().strip() == '0|0|1'
