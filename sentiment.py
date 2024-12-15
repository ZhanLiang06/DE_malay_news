import pandas as pd
from malaya.torch_model.huggingface import Classification
from malaya.supervised.huggingface import load

# 1. Define available Hugging Face model
available_huggingface = {
    'mesolitica/sentiment-analysis-nanot5-tiny-malaysian-cased': {
        'Size (MB)': 93,
        'macro precision': 0.67768,
        'macro recall': 0.68266,
        'macro f1-score': 0.67997,
    },
    'mesolitica/sentiment-analysis-nanot5-small-malaysian-cased': {
        'Size (MB)': 167,
        'macro precision': 0.67602,
        'macro recall': 0.67120,
        'macro f1-score': 0.67339,
    },
}

# 2. Load the sentiment model
model_name = 'mesolitica/sentiment-analysis-nanot5-tiny-malaysian-cased'
sentiment_model = load(
    model=model_name,
    class_model=Classification,
    available_huggingface=available_huggingface,
    force_check=True
)

model_label_mapping = {
    "positive": 1,
    "neutral": 0,
    "negative": -1
}

class Sentiment:
    # 4. Load CSV and extract the "sentences" column
    input_csv_path = "out.csv"  # Replace with your file path
    df = pd.read_csv(input_csv_path)

    if "cleaned_text" not in df.columns:
        raise KeyError("The column 'cleaned_text' is not in the CSV file. Please check the file and column name.")

    # 5. Perform sentiment analysis and handle the result properly
    sentiments = []

    for index, row in df.iterrows():
        cleaned_text = row["cleaned_text"]

        if isinstance(cleaned_text, str):  # Check if it's a string (may need eval or ast.literal_eval)
            cleaned_text = eval(cleaned_text)  # Converts string representation of list to actual list

        # Now `sentences` is a list of tokens, we need to analyze them
        for token in cleaned_text:
            result = sentiment_model.predict(token)

            # Debug: print the result to check its structure
            print(f"Result for token '{token}':", result)

            # Check if result is a list and take the first item if it's a list
            if isinstance(result, list):
                result = result[0]

            # If result is a string (label directly), handle it accordingly
            if isinstance(result, str):
                sentiment_label = model_label_mapping.get(result.lower(), "unknown")
            else:
                sentiment_label = model_label_mapping.get(result.get("label", "").lower(), "unknown")

            sentiments.append({"cleaned_text": token, "sentiment_label": sentiment_label})

    # 6. Create DataFrame from the sentiments
    sentiment_df = pd.DataFrame(sentiments)

    # 7. Save results to a new CSV file
    output_csv_path = "sentiment_analysis_results.csv"  # Specify the desired output file path
    sentiment_df.to_csv(output_csv_path, index=False)

    print(f"Sentiment analysis results saved to {output_csv_path}")
