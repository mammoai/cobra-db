from typing import Tuple, Union

from bson import ObjectId

Id = Union[ObjectId, str]  # used to make methods accept both str or ObjectId

# Loading and processing MRIs

Point3D = Tuple[int, int, int]  # x, y, z
Point2D = Tuple[int, int]  # x, y
Size3D = Tuple[int, int, int]  # x, y, z
Size2D = Tuple[int, int]  # x, y
Rectangle = Tuple[Point2D, Size2D]  # a starting point and two sides size
Cube = Tuple[Point3D, int]  # a starting point and a side size
Box = Tuple[Point3D, Size3D]  # a starting point and sizes for all dimensions
