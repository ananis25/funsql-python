from funsql import Select, TabularNode


def test_query():
    q = Select(10)
    assert isinstance(q, TabularNode)
