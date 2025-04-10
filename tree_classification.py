import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import seaborn as sns
import matplotlib.pyplot as plt
from xgboost import XGBClassifier  # Import XGBoost classifier
from sklearn.preprocessing import LabelEncoder # For potential target encoding

# 1. Sample Dataset Creation (as before)
data = {
    'Account_Age_Months': [12, 36, 6, 48, 24, 60, 18, 30, 42, 54,
                           15, 27, 39, 51, 63, 9, 21, 33, 45, 57],
    'Transaction_Frequency': [5, 15, 2, 20, 10, 25, 7, 18, 12, 22,
                              6, 13, 17, 21, 28, 3, 9, 16, 19, 23],
    'Average_Balance': [1000, 5000, 500, 8000, 3000, 12000, 1500, 6500, 4000, 9500,
                        1200, 4500, 7000, 8500, 15000, 700, 2000, 5500, 7500, 10500],
    'Product_Holdings': [1, 3, 0, 4, 2, 5, 1, 3, 2, 4,
                         1, 2, 3, 4, 5, 0, 1, 3, 3, 4],
    'Interaction_Score': [2, 8, 1, 9, 5, 10, 3, 7, 6, 9,
                          3, 6, 8, 9, 10, 2, 4, 7, 8, 9],
    'High_CASA_Customer': [0, 1, 0, 1, 0, 1, 0, 1, 1, 1,
                           0, 0, 1, 1, 1, 0, 0, 1, 1, 1] # 0: Not High, 1: High
}
df = pd.DataFrame(data)

# 2. Data Preprocessing
X = df.drop('High_CASA_Customer', axis=1)
y = df['High_CASA_Customer']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# 3. Initialize and Train XGBoost Classifier
xgb_classifier = XGBClassifier(
    random_state=42,
    use_label_encoder=False,  # Suppress a common warning
    eval_metric='logloss'    # Specify evaluation metric
    # You can add other hyperparameters here, e.g.,
    # n_estimators=100,
    # learning_rate=0.1,
    # max_depth=3
)
xgb_classifier.fit(X_train, y_train)

# 4. Make Predictions
y_pred = xgb_classifier.predict(X_test)

# 5. Evaluate Performance
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.2f}")

print("\nClassification Report:")
print(classification_report(y_test, y_pred))

print("\nConfusion Matrix:")
sns.heatmap(confusion_matrix(y_test, y_pred), annot=True, fmt='d', cmap='Blues')
plt.title('XGBoost Confusion Matrix')
plt.xlabel('Predicted Label')
plt.ylabel('True Label')
plt.show()

# 6. Visualize Feature Importance (using XGBoost's built-in method)
plt.figure(figsize=(10, 6))
feature_importances = pd.Series(xgb_classifier.feature_importances_, index=X.columns)
feature_importances_sorted = feature_importances.sort_values(ascending=False)
sns.barplot(x=feature_importances_sorted, y=feature_importances_sorted.index)
plt.title('XGBoost Feature Importance')
plt.xlabel('Importance Score')
plt.ylabel('Feature')
plt.show()

# 7. Visualize a Single Tree (optional, requires the native xgboost library)
import xgboost as xgb
plt.figure(figsize=(12, 8))
xgb.plot_tree(xgb_classifier, num_trees=0, rankdir='LR') # rankdir='LR' for horizontal layout
plt.title('First Tree in XGBoost Model')
plt.show()
