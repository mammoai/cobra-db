from typing import Any, Generator, Iterable, List, Tuple


def batcher(iterable: Iterable, batch_size: int) -> Generator[List, None, None]:
    """Create a batched generator out of a generator

    :param generator: the iterable that will be batched.
    :param batch_size: The max size of the list
    :yield: a list with batch_size number of items in iterable. If there is a residual
     at the end, the last list will be shorter.
    """
    batch = []
    counter = 0
    for i in iterable:
        batch.append(i)
        counter += 1
        if counter % batch_size == 0:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch


def add_args_to_iterable(
    iterable: Iterable, *args: Any
) -> Generator[Tuple, None, None]:
    """Create a generator where some args are repeated every time.

    :param iterable: the variable that will change in every iteration
    :yield: a generator that will look like `(iterable[i], args[0], args[1], ...)`
    """
    for i in iterable:
        yield i, *args
