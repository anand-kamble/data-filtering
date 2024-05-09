## Use of LLM to improve plane maintenance

You'll need poetry to create the python virtual environment to run this project.  
> Here you can find how to install poetry: [https://python-poetry.org/docs/](https://python-poetry.org/docs/)

Once you have installed poetry, follow this steps to generate the filtered data.

1. `poetry install` 

2. `poetry shell` 

3. Then Inside the shell run `pip install -U commitizen`
    * This is required only for the first time usage.
    
4. `python src/main.py`
    * Or if you have exited the python shell `poetry run python src/main.py`