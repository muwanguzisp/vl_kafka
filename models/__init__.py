# models/__init__.py
from .base import Base
from .LimsPatient import LimsPatient
from .LimsSample import LimsSample
from .LimsClinician import LimsClinician
from .LimsLabTech import LimsLabTech


__all__ = [
    "Base",
    "LimsPatient",
    "LimsSample",
    "LimsClinician",
    "LimsLabTech",
    
]
