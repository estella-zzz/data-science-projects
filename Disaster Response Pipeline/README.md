# Disaster Response Pipeline Project

### Instructions:
1. Run the following commands in the project's root directory to set up your database and model.

    - To run ETL pipeline that cleans data and stores in database
        `python data/process_data.py data/disaster_messages.csv data/disaster_categories.csv data/DisasterResponse.db`
    - To run ML pipeline that trains classifier and saves
        `python models/train_classifier.py data/DisasterResponse.db models/classifier.pkl`

2. Run the following command in the app's directory to run your web app.
    `python run.py`

3. Go to http://0.0.0.0:3001/

### Project Overview
During disaster events, emergency operation centers are usually flooded with messages with different requests. Processing all information manually would be overwhelming and could cause slow responses. In this project, I created a machine learning pipeline from 26k real messages sent during disaster events, which could classify text messages into 35 categories. Emergency workers can use this app to preprocess the messages and send them to an appropriate disaster relief agency.

### Disaster Message Data
#### Data Overview
The machine learning model is trained using disaster data from [Figure Eight](https://appen.com/). Data contains:
- The original message
- English version message
- genre (direct, news, social)
- categories (1 = message belongs to the category)

#### Data Processing and Feature Selection
The features are extracted from `English version messages`. Each message is tokenized into single words using `nltk`, then lemmatized to further reduce the complexity of features. 
To vectorize the text, we use `Bag of Words` and `TF-IDF` to evaluate the frequency of words appearing in both single messages and across the message pool.

### Model Selection
The classifier used in this project is `Random Forest`. `GridsearchCV` is used to tune the model.
Best params based on grid search:
```
 {'clf__estimator__min_samples_split': 3, 'features__text_pipeline__vect__ngram_range': (1, 2)}
 ```

### Future Improvements
The result shows that the f1 score varies a lot based on the category. This is potentially caused by the distribution of different topics in the training data. The bar chart shows that some of the categories have significantly lower total messages than other categories. From the classification report we can see that `support` has a positive correlation with `recall`. 

For disaster response, identifying all related requests is probably more important than excluding all non-related ones. The next step of this project would be tuning the model to improve the f1 score across all categories by adding new features or trying different classifiers.

### Python and packages version
```
Python
joblib 0.16.0
re 2.2.1
nltk 3.5
json 2.0.9
plotly 4.8.2
pandas 1.0.5
numpy 1.19.0
sklearn 0.23.1
re 2.2.1
flask 1.1.2
```
