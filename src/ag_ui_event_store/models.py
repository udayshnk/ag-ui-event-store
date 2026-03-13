from typing import Optional
from pydantic import BaseModel


class Thread(BaseModel):
    thread_id: str
    namespace: Optional[str]
    title: Optional[str]
    user_message: Optional[str]
    latest_run_id: Optional[str] = None
    created_at: int
    updated_at: int


class Run(BaseModel):
    run_id: str
    thread_id: str
    parent_run_id: Optional[str]
    previous_run_id: Optional[str] = None
    seq: int
    status: str  # running | completed | error
    title: Optional[str]
    summary: Optional[str]
    created_at: int
    updated_at: int


class Event(BaseModel):
    run_id: str
    seq: int
    event_type: str
    data: dict
    created_at: int
