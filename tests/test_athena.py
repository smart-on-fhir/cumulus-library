"""Tests for Athena database support"""

from cumulus_library import databases


def test_schema_parsing():
    # A sample response for table `observation`, column `component`.
    # (The original did not have any spaces.)
    schema = {
        "simple_field": "varchar",
        "simple_row": "row(id varchar, end varchar, start varchar)",
        "simple_array": "array(varchar)",
        "complex": """array(row(
            coding array(row(code varchar, display row(text varchar),
                             system varchar, userselected boolean, id varchar,
                             version varchar)),
            text varchar,
            id varchar
        ))""",
    }
    expected = {
        "simple_field": {},
        "simple_row": {"id": {}, "end": {}, "start": {}},
        "simple_array": {},
        "complex": {
            "coding": {
                "code": {},
                "display": {"text": {}},
                "system": {},
                "userselected": {},
                "id": {},
                "version": {},
            },
            "text": {},
            "id": {},
        },
    }
    parser = databases.AthenaParser()
    assert expected == parser.parse_found_schema(schema)
