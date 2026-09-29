import uvicorn

from app.api import create_app
from app.datasource import SimulatedDataSource

app = create_app(source=SimulatedDataSource({"hello": "world"}))


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
