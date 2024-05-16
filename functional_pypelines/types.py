from typing import Dict, List, Union

JSONScalar = Union[int, float, bool, str, None]
JSONType = Union[JSONScalar, Dict[str, "JSONType"], List["JSONType"]]
