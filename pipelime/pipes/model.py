import copy
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple
import re
from pydantic import BaseModel
import pydash
import rich
from pipelime.tools.dictionaries import DictionaryUtils
import networkx as nx
import itertools


class NodeModel(BaseModel):
    command: str
    args: dict = {}
    inputs: dict = {}
    outputs: dict = {}


class NodesModel(BaseModel):
    nodes: Dict[str, NodeModel]
