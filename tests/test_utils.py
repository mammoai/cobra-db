from cobra_db.scripts.utils import add_args_to_iterable, batcher
from cobra_db.utils import intersect_dicts, intersect_dicts_allow_empty_minority


def test_intersect_dicts():
    d1 = dict(a=1, b=2, c=3)
    d2 = dict(a=1, b=2)
    group_1 = [d1, d2]
    assert intersect_dicts(group_1) == d2


def test_intersect_dicts_allow_empty_minority():
    d1 = dict(a=1, b=2, c=3, e=4)
    d2 = dict(a=1, b=2)
    d3 = dict(a=1, b=0, c=3, d=None)
    group = [d1, d2, d3]

    ans = dict(a=1, c=3)
    assert intersect_dicts_allow_empty_minority(group) == ans


def test_batcher():
    my_list = [1, 2, 3, 4, 5]
    batched = batcher(my_list, 2)
    batches = [[1, 2], [3, 4], [5]]
    for my_batch, expected_batch in zip(batched, batches):
        assert my_batch == expected_batch


def test_add_args_to_iterable():
    my_list = [1, 2, 3]
    my_args = "a", "b", "c"
    generator = add_args_to_iterable(my_list, *my_args)
    for i, v in zip(generator, my_list):
        assert i == (v, *my_args)
