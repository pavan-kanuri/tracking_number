import uuid
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pika
from cassandra.cluster import Cluster

app = FastAPI()

# Initialize Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('tracking')


class TrackingNumberResponse(BaseModel):
    tracking_number: str


@app.post("/generate-tracking-number", response_model=TrackingNumberResponse)
async def generate_tracking_number():
    try:
        # Generate a UUID
        unique_id = str(uuid.uuid4())
        # Generate a timestamp
        timestamp = str(int(time.time() * 1000))
        # Combine UUID and timestamp
        tracking_number = f"{unique_id}-{timestamp}"

        # Save to database
        if not save_to_database(tracking_number):
            raise HTTPException(status_code=500, detail="Failed to save tracking number")

        # Send to queue for further processing if needed
        send_to_queue(tracking_number)

        return TrackingNumberResponse(tracking_number=tracking_number)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def save_to_database(tracking_number: str) -> bool:
    try:
        session.execute(
            """
            INSERT INTO tracking_numbers (id, tracking_number)
            VALUES (uuid(), %s)
            """,
            (tracking_number,)
        )
        return True
    except Exception as e:
        print(f"Database Error: {e}")
        return False


def send_to_queue(tracking_number: str):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='tracking_queue')
        channel.basic_publish(exchange='',
                              routing_key='tracking_queue',
                              body=tracking_number)
        connection.close()
    except Exception as e:
        print(f"Queue Error: {e}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
