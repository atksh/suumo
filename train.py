import matplotlib.pyplot as plt
import numpy as np
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd

df = pd.read_csv("cleaned.csv")
X = df.iloc[:, 6:]
nostd_cols = []
for col in X.columns:
    if X[col].std() == 0:
        nostd_cols.append(col)
X = X.drop(nostd_cols, axis=1)
y = df["monthly_price"] * 24 + df["initial_cost"] + df["proxy_cost"]
y = np.log1p(y)
regr = linear_model.ElasticNetCV(cv=5, random_state=0)
regr.fit(X, y)
coef = pd.DataFrame(regr.coef_ * 100, X.columns, columns=["Coefficient"])
print(1 - ((coef["Coefficient"] == 0).sum() / len(coef)))
coef.to_csv("coef.csv")
