from typing import Dict
from pydantic import BaseModel


class NodeModel(BaseModel):
    command: str
    args: dict = {}
    inputs: dict = {}
    outputs: dict = {}


class DAGModel(BaseModel):
    nodes: Dict[str, NodeModel]
