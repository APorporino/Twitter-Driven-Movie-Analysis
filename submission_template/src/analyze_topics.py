import pandas as pd
import json
import argparse
import math

STOP_WORDS = ["and", "or", "a", "was", "is", "really", "she's","e",
              "her", "his", "shes", "go"]
CHARS_TO_REMOVE = ["#", "@", "\n", ":", ",", ".", "-", "_", "\\", "?","(", ")", "\""]
NUM_WORDS = 10


def idf(document, word):
    """
    This method returns the idf score given a document and a word.
    :param document:
    :param word:
    :return:
    """
    num_documents = len(document.keys())

    documents_with_word = 0

    for d in document:
        if word in document[d].keys():
            documents_with_word += 1

    return math.log(num_documents / documents_with_word, 10)


def calculate_tfidf(word_count, topic, idf_dictionary, num_words):
    """
    This method will return a list of size 'num_words' representing the words
    that 'topic' had with highest ifidf score.

    :param word_count: a JSON file of ponies with word counts.
    :param pony: Name of pony in question
    :param idf_dictionary: Dictionary representing previously calculated idf scores for words
    :param num_words:
    :return:
    """
    # Dictionary holding outputwords as keys and tf_idf score as value
    output_words = {}

    for word in word_count[topic]:
        tf = word_count[topic][word]
        if word in idf_dictionary:
            idf_score = idf_dictionary[word]
        else:
            idf_score = idf(word_count, word)
            idf_dictionary[word] = idf_score

        tf_idf = tf * idf_score

        if len(output_words) == num_words:
            min_key = min(output_words.items(), key=lambda x: x[1])[0]
            if tf_idf > output_words[min_key]:
                output_words.pop(min_key)
                output_words[word] = tf_idf
        else:
            output_words[word] = tf_idf

    ranked_output_words = dict(sorted(output_words.items(), key=lambda item: item[1]))
    return list(ranked_output_words.keys())[::-1]


def clean_data(df):
    # This removes all NON ASCII charaters
    df["text"] = df["text"].apply(lambda x: x.encode("ascii", "ignore").decode())
    translate_dict = str.maketrans({k: " " for k in CHARS_TO_REMOVE})
    df["text"] = df["text"].apply(lambda x: x.translate(translate_dict))
    # make everything lowercase
    df["text"] = df["text"].apply(lambda x: x.lower())
    df["Topic"] = df["Topic"].apply(lambda x: x.lower())

    return df


def process_line(topic, dialog, topics):

    for word in dialog.split(" "):
        if word in STOP_WORDS:
            continue
        if word in topics[topic].keys():
            topics[topic][word] += 1
        else:
            topics[topic][word] = 1

    return topics


def main():
    # Parse arguments: -i = input csv file, -o = output json file
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", required=True)
    parser.add_argument("-o", required=True)
    args = parser.parse_args()

    # Load CSV
    df = pd.read_csv(args.i)
    # Clean Data
    df = clean_data(df)

    topic_counts = {
        "box office": {},
        "opinion": {},
        "family williams": {},
        "family": {},
        "social concerns": {},
        "will smith": {},
        "other": {},
        "planning or watching the movie": {}
    }

    for index, row in df.iterrows():
        process_line(row["Topic"], row["text"], topic_counts)

    # Now we have counts for topics - we can find tf-idf

    topic_top_tf_idf_words = {
        "box office": [],
        "opinion": [],
        "family williams": [],
        "family": [],
        "social concerns": [],
        "will smith": [],
        "other": [],
        "planning or watching the movie": []
    }

    idf_dictionary = {}
    for topic, word_count_dict in topic_counts.items():
        # Remove space word in counts
        if "" in word_count_dict.keys():
            word_count_dict.pop("")
        topic_top_tf_idf_words[topic] = calculate_tfidf(topic_counts, topic, idf_dictionary, NUM_WORDS)


    with open(args.o, "w") as fp:
        json.dump(topic_top_tf_idf_words, fp, indent=4)

if __name__ == "__main__":
    main()
