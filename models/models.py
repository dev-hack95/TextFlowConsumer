from typing import List
from pydantic import BaseModel, StringConstraints
from typing_extensions import Annotated


class Textflow(BaseModel):
    id: int
    video: Annotated[str, StringConstraints(max_length=20)]
    audio: Annotated[str, StringConstraints(max_length=63)]
    text: List[Annotated[str, StringConstraints(max_length=20000)]]
