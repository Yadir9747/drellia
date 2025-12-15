# models.py

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class NormalizedMessage:
    ts_ms: int                  
    actor_type: str          
    actor_email: Optional[str]  
    content: str                
    raw: Dict[str, Any]        

@dataclass
class ConversationSegment:
    segment_index: int
    employee_type: str            
    employee_email: Optional[str]  
    messages: List[NormalizedMessage]

    @property

    def original_ts_ms(self) -> int:
        return min(m.ts_ms for m in self.messages) if self.messages else 0