# Shop-Suggest

Adding Business elements to the General Product Recommendation System specific to e-
commerce

## Features
1. User Level Recommendations
    * You may also like (personalized recommendations)
    * You might be interested in (viewing/browsing history recommendations)
2. Item Level Recommendations
    * Similar Products (based on product being viewed, show list of similar products)
        * Attribute Based
        * Image Based (Visual Similarity) (To be developed in future)
    * Frequently Bought Together
3. Category/Sub-Category Bestsellers

## How to setup repo and start API?
Works with python3.7<br>
Few system packages to be installed (gcc, python3.7-dev, python3.7-venv, unixodbc-dev)<br>
<br>
python3.7 -m venv env<br>
source env/bin/activate<br>
pip install --upgrade pip<br>
pip install -r requirements.txt<br>
uvicorn server:app --host 0.0.0.0 --port 8000<br>


### .env file
LOGGING_LEVEL="{logging level}"
