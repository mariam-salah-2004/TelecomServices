from fastapi import FastAPI
from connection import send_to_event_hub, generate_telecom_event

app = FastAPI()


@app.get("/generate")
def generate_event(event_type: str = None):
    # One event per call: random type, or pass ?event_type=sale (usage / recharge / wallet_transaction / subscription / sale / corporate_order)
    event = generate_telecom_event(event_type)
    result = send_to_event_hub(event)
    return {"result": result, "event": event}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
