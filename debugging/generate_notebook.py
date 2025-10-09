import json

notebook = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "cells": [
        {
            "cell_type": "code",
            "source": ["# Welcome to your new notebook\n# Type here in the cell editor to add code!\n"],
            "execution_count": None,
            "outputs": [],
            "metadata": {}
        },
        {
            "cell_type": "markdown",
            "source": ["This is a sample Jupyter notebook created to demonstrate the upload process to Microsoft Fabric.\n\nYou can add more cells and content as needed."],
            "metadata": {}
        },
        {
            "cell_type": "code",
            "source": ["print(\"hello, world!\")"],
            "execution_count": None,
            "outputs": [],
            "metadata": {}
        },
    ],
    "metadata": {
        "language_info": {
            "name": "python"
        }
    }
}

with open("notebooks/test_notebook.ipynb", "w", encoding="utf-8") as f:
    json.dump(notebook, f, indent=4)

print("test_notebook.ipynb has been created!")
