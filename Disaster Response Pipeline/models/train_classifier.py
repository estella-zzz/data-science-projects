import sys
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine

import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.metrics import confusion_matrix
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.multioutput import MultiOutputClassifier
from sklearn.metrics import classification_report

import nltk
from nltk.corpus import stopwords
nltk.download(['stopwords','punkt', 'wordnet'])
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import word_tokenize

import pickle


def load_data(database_filepath):
    # load data from database
    engine = create_engine('sqlite:///%s' % database_filepath)
    df = pd.read_sql_table('DisasterResponse', engine)
    
    # remove invalid values from 'related'
    df = df.loc[df['related'] != 2]

    # remove topics that don't have any positive results
    rm_list = []
    for col in df.columns[4:]:
        if df[col].sum() == 0:
            rm_list.append(col)
    df = df.drop(columns = rm_list)
    
    X = df.message.values
    y_data = df.drop(columns = ['id', 'message', 'original', 'genre'])
    y = y_data.values
    category_names = y_data.columns.tolist()
    
    return X, y, category_names


def tokenize(text):
    # normalize the text
    text = re.sub(r"[^a-zA-Z0-9]", " ", text.lower())

    # tokenize and lemmatize
    tokens = word_tokenize(text)
    lemmatizer = WordNetLemmatizer()
    
    clean_tokens = []
    for tok in tokens:
        clean_tok = lemmatizer.lemmatize(tok).strip()
        clean_tokens.append(clean_tok)

    return(clean_tokens)


# build pipeline with features, classifier and grid search
def build_model():
    pipeline = Pipeline([
        ('features', FeatureUnion([

            ('text_pipeline', Pipeline([
                ('vect', CountVectorizer(tokenizer=tokenize)),
                ('tfidf', TfidfTransformer())
            ]))
        ])),
        ('clf', MultiOutputClassifier(RandomForestClassifier()))
    ])
    
    parameters = {
        'features__text_pipeline__vect__ngram_range': ((1, 1), (1, 2)),
        'clf__estimator__min_samples_split': [2, 3]
        }
    cv = GridSearchCV(pipeline, param_grid=parameters, n_jobs = -1, verbose = 1)
    
    return cv


# evaluate model score
def evaluate_model(model, X_test, Y_test, category_names):
    Y_pred = model.predict(X_test)
    
    category_names = category_names
    cl_report = classification_report(Y_test, Y_pred, target_names = category_names)
    best_score = model.best_estimator_.score(X_test, Y_test)

    print("Labels:", category_names)
    print("Classification Report:\n", cl_report)
    print("Best Score:", best_score)
    print("\nBest Parameters:", model.best_params_)

#save model to target dictionary
def save_model(model, model_filepath):
    filename = model_filepath
    pickle.dump(model, open(filename, 'wb'))

def main():
    if len(sys.argv) == 3:
        database_filepath, model_filepath = sys.argv[1:]
        print('Loading data...\n    DATABASE: {}'.format(database_filepath))
        X, Y, category_names = load_data(database_filepath)
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2)
        
        print('Building model...')
        model = build_model()
        
        print('Training model...')
        model.fit(X_train, Y_train)
        
        print('Evaluating model...')
        evaluate_model(model, X_test, Y_test, category_names)

        print('Saving model...\n    MODEL: {}'.format(model_filepath))
        save_model(model, model_filepath)

        print('Trained model saved!')

    else:
        print('Please provide the filepath of the disaster messages database '\
              'as the first argument and the filepath of the pickle file to '\
              'save the model to as the second argument. \n\nExample: python '\
              'train_classifier.py ../data/DisasterResponse.db classifier.pkl')


if __name__ == '__main__':
    main()