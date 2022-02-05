from typing import Dict, Optional
from pydantic import BaseModel


class NodeModel(BaseModel):
    command: str
    args: dict = {}
    inputs: dict = {}
    outputs: dict = {}
    outputs_schema: Optional[dict] = None
    inputs_schema: Optional[dict] = None


class DAGModel(BaseModel):
    nodes: Dict[str, NodeModel]
