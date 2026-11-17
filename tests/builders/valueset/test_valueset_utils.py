from cumulus_library.builders.valueset import valueset_utils


def test_valueset_config_empty_stewards_when_not_specified():
    valueset_config = valueset_utils.ValuesetConfig()

    assert valueset_config.umls_stewards == {}
    assert valueset_config.vsac_stewards == {}


def test_valueset_config_empty_stewards_when_none():
    valueset_config = valueset_utils.ValuesetConfig(
        umls_stewards=None,
        vsac_stewards=None,
    )

    assert valueset_config.umls_stewards == {}
    assert valueset_config.vsac_stewards == {}
