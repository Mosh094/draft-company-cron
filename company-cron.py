import psycopg2
import openai
import os
import re
from urllib.parse import urlparse
import json


def load_gics_data(json_file):
    with open(json_file, "r") as f:
        gics_data = json.load(f)

    return gics_data


gics_data = load_gics_data("gics_data.json")


def find_and_validate_gics_value(gics_value, gics_data, gics_column):
    for entry in gics_data:
        if gics_value.lower() in entry[gics_column].lower():
            return entry[gics_column]

    return None


def find_and_validate_gics_value(gics_response, gics_data, gics_column):
    for entry in gics_data:
        if entry[gics_column].lower() in gics_response.lower():
            return entry[gics_column]

    return None


# Updating database credentials
DB_NAME = "df7n96sahm849q"
DB_USER = "ejesvaacpduvgt"
DB_PASSWORD = "797dbf4dcf91d5c399df0a719cf7d73c99f6fb3bc09d7fc384627833e89ca9f9"
DB_HOST = "ec2-34-197-84-74.compute-1.amazonaws.com"
DB_PORT = "5432"

# Setting OpenAI API-key
openai.api_key = "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"


def get_missing_data():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()

    query = """
        SELECT id, name, url, linkedin_url, gics_sectors, gics_industry_groups, gics_industries, gics_sub_industries
        FROM company
        WHERE url IS NULL OR linkedin_url IS NULL OR gics_sectors IS NULL OR gics_industry_groups IS NULL
            OR gics_industries IS NULL OR gics_sub_industries IS NULL;
    """
    cursor.execute(query)
    return cursor.fetchall()


def extract_url(response_text):
    url_pattern = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    urls = re.findall(url_pattern, response_text)

    if len(urls) > 0:
        return urls[0]
    else:
        return None


def extract_gics(response_text):
    words = response_text.split()
    cleaned_words = [word.strip(',.!?') for word in words]
    return ' '.join(cleaned_words)


def get_chatgpt_response(prompt, response_type):
    response = openai.Completion.create(
        engine="text-davinci-002",
        prompt=prompt,
        max_tokens=50,
        n=1,
        stop=None,
        temperature=0.5,
    )

    message = response.choices[0].text.strip()

    if message.lower() in ["i don't know", "i am not sure", "i cannot answer that", "i cannot answer that", "is not known", "is not found", "is not available", "I do not know"]:
        return None
    else:
        if response_type == "url" or response_type == "linkedin_url":
            return extract_url(message)
        elif response_type in ["gics_sectors", "gics_industry_groups", "gics_industries", "gics_sub_industries"]:
            return extract_gics(message)
        else:
            return message


def update_data(id, column_name, value):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()

    query = f"UPDATE company SET {column_name} = %s WHERE id = %s;"
    cursor.execute(query, (value, id))
    conn.commit()


def main():
    missing_data = get_missing_data()

    for data in missing_data:
        id = data[0]
        name = data[1]

        url = data[2]
        linkedin_url = data[3]
        gics_sectors = data[4]
        gics_industry_groups = data[5]
        gics_industries = data[6]
        gics_sub_industries = data[7]

        if url is None:
            prompt = f"What is the website URL of the company '{name}'?"
            url = get_chatgpt_response(prompt, "url")
            update_data(id, "url", url)

        if linkedin_url is None:
            prompt = f"What is the LinkedIn URL of the company '{name}'?"
            linkedin_url = get_chatgpt_response(prompt, "linkedin_url")
            update_data(id, "linkedin_url", linkedin_url)

        if gics_sub_industries is None:
            prompt = f"Can you classify '{name}' based on the Global Industry Classification Standard (GICS) classification under gics_sub_industries?"
            gics_sub_industries_response = get_chatgpt_response(
                prompt, "gics_sub_industries")
            gics_sub_industries = find_and_validate_gics_value(
                gics_sub_industries_response, gics_data, "gics_sub_industries")

            if gics_sub_industries is not None:
                for entry in gics_data:
                    if entry["gics_sub_industries"] == gics_sub_industries:
                        update_data(id, "gics_sectors", entry["gics_sectors"])
                        update_data(id, "gics_industry_groups",
                                    entry["gics_industry_groups"])
                        update_data(id, "gics_industries",
                                    entry["gics_industries"])
                        update_data(id, "gics_sub_industries",
                                    entry["gics_sub_industries"])
                        break


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        print(traceback.format_exc())
