import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
import time

# 1. Sample Dataset Creation (with a categorical column)
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
    'Customer_Segment': ['A', 'B', 'A', 'C', 'B', 'C', 'A', 'B', 'C', 'A',
                         'B', 'A', 'C', 'B', 'A', 'C', 'B', 'A', 'C', 'B'],
    'High_CASA_Customer': [0, 1, 0, 1, 0, 1, 0, 1, 1, 1,
                           0, 0, 1, 1, 1, 0, 0, 1, 1, 1] # 0: Not High, 1: High
}
df = pd.DataFrame(data)

# 2. Identify and Perform One-Hot Encoding on Categorical Columns
categorical_cols = ['Customer_Segment']
df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

# 3. Data Preprocessing
X = df.drop('High_CASA_Customer', axis=1)
y = df['High_CASA_Customer']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# 4. Initialize Models with Hyperparameters
decision_tree = DecisionTreeClassifier(
    random_state=42,
    max_depth=5,             # Maximum depth of the tree
    min_samples_split=2,     # Minimum number of samples required to split an internal node
    min_samples_leaf=1       # Minimum number of samples required to be at a leaf node
)

random_forest = RandomForestClassifier(
    random_state=42,
    n_estimators=100,        # Number of decision trees in the forest
    max_depth=10,            # Maximum depth of each tree
    min_samples_split=5,     # Minimum number of samples required to split an internal node
    min_samples_leaf=2,      # Minimum number of samples required to be at a leaf node
    class_weight='balanced'  # Adjust weights inversely proportional to class frequencies
)

xgboost_model = XGBClassifier(
    random_state=42,
    n_estimators=100,        # Number of boosting rounds (trees)
    learning_rate=0.1,       # Step size shrinkage to prevent overfitting
    max_depth=3,             # Maximum depth of a tree
    subsample=0.8,           # Fraction of samples to be used for fitting the individual base learners
    colsample_bytree=0.8,    # Fraction of features to be considered when building each tree
    use_label_encoder=False,  # Suppress a warning in recent versions
    eval_metric='logloss'    # Metric to be used for validation data
)

models = {
    'Decision Tree': decision_tree,
    'Random Forest': random_forest,
    'XGBoost': xgboost_model
}

results = {}

# 5. Train and Evaluate Models
for name, model in models.items():
    start_time = time.time()
    model.fit(X_train, y_train)
    end_time = time.time()
    train_time = end_time - start_time

    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, output_dict=True)
    confusion = confusion_matrix(y_test, y_pred)

    results[name] = {
        'accuracy': accuracy,
        'classification_report': report,
        'confusion_matrix': confusion,
        'train_time': train_time
    }

# 6. Display Performance Comparison
print("Performance Comparison of Models:")
for name, result in results.items():
    print(f"\n--- {name} ---")
    print(f"Accuracy: {result['accuracy']:.2f}")
    print("Classification Report:")
    print(pd.DataFrame(result['classification_report']).transpose())
    print("\nConfusion Matrix:")
    sns.heatmap(result['confusion_matrix'], annot=True, fmt='d', cmap='Blues')
    plt.title(f'Confusion Matrix - {name}')
    plt.xlabel('Predicted Label')
    plt.ylabel('True Label')
    plt.show()
    print(f"Training Time: {result['train_time']:.4f} seconds")

# 7. Visualize Feature Importance (for models that provide it)
plt.figure(figsize=(15, 6))
feature_names = X.columns

if hasattr(decision_tree, 'feature_importances_'):
    plt.subplot(1, 3, 1)
    importance_dt = pd.Series(decision_tree.feature_importances_, index=feature_names)
    importance_dt_sorted = importance_dt.sort_values(ascending=False)
    sns.barplot(x=importance_dt_sorted, y=importance_dt_sorted.index)
    plt.title('Decision Tree Feature Importance')

if hasattr(random_forest, 'feature_importances_'):
    plt.subplot(1, 3, 2)
    importance_rf = pd.Series(random_forest.feature_importances_, index=feature_names)
    importance_rf_sorted = importance_rf.sort_values(ascending=False)
    sns.barplot(x=importance_rf_sorted, y=importance_rf_sorted.index)
    plt.title('Random Forest Feature Importance')

if hasattr(xgboost_model, 'feature_importances_'):
    plt.subplot(1, 3, 3)
    importance_xgb = pd.Series(xgboost_model.feature_importances_, index=feature_names)
    importance_xgb_sorted = importance_xgb.sort_values(ascending=False)
    sns.barplot(x=importance_xgb_sorted, y=importance_xgb_sorted.index)
    plt.title('XGBoost Feature Importance')

plt.tight_layout()
plt.show()

# 8. Visualize a Single Tree from Decision Tree and Random Forest (for inspection)
plt.figure(figsize=(15, 5))

# Decision Tree
plt.subplot(1, 2, 1)
plot_tree(decision_tree, feature_names=X.columns, class_names=['Not High CASA', 'High CASA'], filled=True, max_depth=2) # Limiting depth
plt.title('Decision Tree Visualization (Max Depth 2)')

# Random Forest (visualize the first tree)
if random_forest.estimators_:
    plt.subplot(1, 2, 2)
    plot_tree(random_forest.estimators_[0], feature_names=X.columns, class_names=['Not High CASA', 'High CASA'], filled=True, max_depth=2) # Limiting depth
    plt.title('First Tree in Random Forest (Max Depth 2)')
else:
    print("\nRandom Forest model has no estimators to visualize (this should not happen).")

plt.tight_layout()
plt.show()

# 9. Visualize a Single Tree from XGBoost
import xgboost as xgb
plt.figure(figsize=(12, 8))
xgb.plot_tree(xgboost_model, num_trees=0, max_depth=2) # Plot the first tree with limited depth
plt.title('First Tree in XGBoost Ensemble (Max Depth 2)')
plt.show()
