import pandas as pd
from malaya.torch_model.huggingface import Classification
from malaya.supervised.huggingface import load

# 1. Define available Hugging Face models
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

# 2. Load the sentiment models
model_names = [
    'mesolitica/sentiment-analysis-nanot5-tiny-malaysian-cased',
    'mesolitica/sentiment-analysis-nanot5-small-malaysian-cased'
]

sentiment_models = [load(
    model=model_name,
    class_model=Classification,
    available_huggingface=available_huggingface,
    force_check=True
) for model_name in model_names]

model_label_mapping = {
    "positive": 1,
    "neutral": 0,
    "negative": -1
}

class Sentiment:
    # 4. Load CSV and extract the "sentences" column
    input_csv_path = "out.csv"  # Replace with your file path
    df = pd.read_csv(input_csv_path)
    
    if "filtered_tokens" not in df.columns:
        raise KeyError("The column 'filtered_tokens' is not in the CSV file. Please check the file and column name.")
    
    # 5. Perform sentiment analysis and handle the result properly
    sentiments = []
    
    for index, row in df.iterrows():
        filtered_tokens = row["filtered_tokens"]
        
        if isinstance(filtered_tokens, str):  # Check if it's a string (may need eval or ast.literal_eval)
            filtered_tokens = eval(filtered_tokens)  # Converts string representation of list to actual list
        
        # Now filtered_tokens is a list of tokens, we need to analyze them
        for token in filtered_tokens:
            model_predictions = []
            # Get predictions from each model
            for sentiment_model in sentiment_models:
                result = sentiment_model.predict(token)
                
                # Debug: print the result to check its structure
                print(f"Result for token '{token}':", result)
                
                # Check if result is a list and take the first item if it's a list
                if isinstance(result, list):
                    result = result[0]
                
                # If result is a string (label directly), handle it accordingly
                if isinstance(result, str):
                    sentiment_label = model_label_mapping.get(result.lower(), "unknown")
                    score = None  # No score if the result is just a label
                else:
                    sentiment_label = model_label_mapping.get(result.get("label", "").lower(), "unknown")
                    score = result.get("score", None)
                
                model_predictions.append((sentiment_label, score))

            # Aggregate the predictions (here we use majority voting)
            # If there are scores, average them
            sentiment_labels = [pred[0] for pred in model_predictions]
            sentiment_scores = [pred[1] for pred in model_predictions if pred[1] is not None]

            # Majority voting for labels
            final_label = max(set(sentiment_labels), key=sentiment_labels.count)

            # Average score (if available)
            final_score = None
            if sentiment_scores:
                final_score = sum(sentiment_scores) / len(sentiment_scores)

            sentiments.append({"filtered_tokens": token, "sentiment_label": final_label, "sentiment_score": final_score})
    
    # 6. Create DataFrame from the sentiments
    sentiment_df = pd.DataFrame(sentiments)
    
    # 7. Save results to a new CSV file
    output_csv_path = "word_sentiment.csv"  # Specify the desired output file path
    sentiment_df.to_csv(output_csv_path, index=False)
    
    print(f"Sentiment analysis results saved to {output_csv_path}")