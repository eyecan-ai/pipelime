from typing import Dict
from pydantic import BaseModel


class NodeModel(BaseModel):
    command: str
    args: dict = {}
    inputs: dict = {}
    outputs: dict = {}


class NodesModel(BaseModel):
    nodes: Dict[str, NodeModel]
