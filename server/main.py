from fastapi import FastAPI
from models import Restaurant, State
from typing import List


app = FastAPI()

restaurants: List[Restaurant] = [
    Restaurant(
        id=1,
        name="Sushi Haven",
        state="Tokyo",
        description="Authentic Japanese sushi with fresh ingredients and a cozy ambiance.",
        location="Tokyo, Japan",
        ratings=4.8
    ),
    Restaurant(
        id=2,
        name="The Burger Joint",
        state="New York",
        description="Handcrafted gourmet burgers with house-made sauces and crispy fries.",
        location="New York, USA",
        ratings=4.5
    ),
    Restaurant(
        id=3,
        name="Pasta Paradise",
        state="Lazio",
        description="Traditional Italian pasta dishes made with fresh, local ingredients.",
        location="Rome, Italy",
        ratings=4.7
    )
]

states: List[State] = [
    State(id=1, name="California", code="CA", num_restaurants=15000),
    State(id=2, name="New York", code="NY", num_restaurants=12000),
    State(id=3, name="Texas", code="TX", num_restaurants=11000),
    State(id=4, name="Florida", code="FL", num_restaurants=9000),
    State(id=5, name="Illinois", code="IL", num_restaurants=8000),
    State(id=6, name="Washington", code="WA", num_restaurants=7000),
    State(id=7, name="Nevada", code="NV", num_restaurants=5000),
    State(id=8, name="Arizona", code="AZ", num_restaurants=4500),
    State(id=9, name="Massachusetts", code="MA", num_restaurants=4000),
    State(id=10, name="Colorado", code="CO", num_restaurants=3500),
]

@app.get("/")
def read_root():
    return {
        "message": "Welcome to Restaurant Recommender System"
    }

@app.get("/states")
def list_states() -> List[State]:
    return states

@app.get("/restaurants")
def list_restaurants(state: str = "") -> List[Restaurant]:
    if state:
        response = list(filter(lambda x: x.state == state, restaurants))
    else:
        response = restaurants
    return response

@app.post("/restaurants/{restaurant_id}/recommend")
def recommend_restaurants(restaurant_id: int, top_k: int = 5) -> List[Restaurant]:
    return restaurants
    