from pydantic import BaseModel

class Restaurant(BaseModel):
    id: int
    name: str
    state: str
    description: str
    location: str
    ratings: float

class State(BaseModel):
    id: int
    name: str
    code: str
    num_restaurants: int