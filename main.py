from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from kafka import KafkaProducer
import json

app = FastAPI()
producer = KafkaProducer(bootstrap_servers='kafka:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         key_serializer=lambda v: v.encode('utf-8'))


class Property(BaseModel):
    id: int
    business_type: str
    event_type: str




@app.post("/properties/")
async def create_item(item: Property):
    producer.send(topic='test.events', key='a', value={"action": "create", "item": item.model_dump()})
    return item


# Update an item
@app.put("/properties/{property_id}", response_model=Property)
async def update_item(property_id: int, prop: Property):
    producer.send(topic='test.events', key='a', value={"action": "update", "item": prop.model_dump()})
    return prop


# Delete an item
@app.delete("/properties/{property_id}")
async def delete_item(item_id: int):
    producer.send(topic='test.events', key='a', value={"action": "delete", "item_id": item_id})
    return {"result": "Item deleted"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
