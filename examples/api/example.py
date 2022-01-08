from fastapi.applications import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pipelime.sequences.api.underfolder import UnderfolderAPI


def auth_callback(token: str) -> bool:
    return token == "miao"


mapp = UnderfolderAPI(
    {
        "data0": "../data/underfolder/example_dataset",
        "data1": "../data/underfolder/example_dataset",
        "data2": "../data/underfolder/example_dataset",
    },
    auth_callback=auth_callback,
)

app = FastAPI()
app.include_router(mapp)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# uvicorn.run(
#     "app",
#     host="0.0.0.0",
#     port=8000,
#     reload=True,
#     debug=True,
#     # workers=3,
# )
