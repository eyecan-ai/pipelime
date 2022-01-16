import click
from fastapi.applications import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pipelime.sequences.api.underfolder import UnderfolderAPI
from fastapi.staticfiles import StaticFiles
import uvicorn
import webbrowser
import threading


@click.command("underfolder_api_server")
@click.option("--host", default="127.0.0.1", help="Host to run the server on.")
@click.option("--port", default=8099, help="Port to run the server on.")
@click.option(
    "-i",
    "--input_folder",
    default="../../../tests/sample_data/datasets/underfolder_minimnist",
    help="Input dataset folder",
)
@click.option(
    "-n",
    "--name",
    default="underfolder_api_server",
    help="Name of the dataset endpoint.",
)
def underfolder_api_server(host: str, port: int, input_folder: str, name: str):

    # creates the api base url
    baseUrl = "http://" + host + ":" + str(port)

    # UnderfolderAPI endpoints
    endpoint = UnderfolderAPI({name: input_folder}, auth_callback=None)

    # FastAPI app
    app = FastAPI()

    # FastAPI static files provider (for serving static files like index.html)
    app.mount("/static", StaticFiles(directory=".", html=True), name="static")

    # Add UnderfolderAPI microservice to main app
    app.include_router(endpoint)

    # Add CORS middleware (for allowing cross-origin requests)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Open browser with 2.0 seconds delay
    threading.Timer(
        2.0, webbrowser.open, args=(f"{baseUrl}/static/index.html",)
    ).start()

    # Run ASGI webserver
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8099,
        debug=True,
    )


if __name__ == "__main__":
    underfolder_api_server()
