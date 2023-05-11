import os
import openai
import psycopg2
import psycopg2.extras
import json
import re
from dotenv import load_dotenv

load_dotenv()

database_url = os.getenv("DATABASE_URL")
openai_api_key = os.getenv("OPENAI_API_KEY")

# Connect to your postgres DB
conn = psycopg2.connect(database_url, sslmode='require')
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Set OpenAI API key
openai.api_key = openai_api_key

# Load GICS data
with open('gics_data.json', 'r') as f:
    gics_data = json.load(f)


def get_missing_data():
    cur.execute(
        "SELECT * FROM companies WHERE url IS NULL OR linkedin_url IS NULL OR gics_sub_industries IS NULL;")
    return cur.fetchall()


def update_data(id, column, value):
    try:
        cur.execute(
            f"UPDATE companies SET {column} = %s WHERE id = %s;", (value, id))
        conn.commit()
    except psycopg2.Error as e:
        print(f"An error occurred while updating the database: {e}")


def get_chatgpt_response(prompt, response_type):
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=50,
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


def main():
    try:
        missing_data = get_missing_data()

        for data in missing_data:
            id = data['id']
            name = data['name']

            url = data['url']
            if url is None:
                print(f"Updating URL for company {name}")
                prompt = f"What is the website URL of the company '{name}'?"
                url_response = get_chatgpt_response(prompt, 'url')
                if url_response:
                    print(f"URL response for company {name}: {url_response}")
                    cleaned_url = extract_url(url_response)
                    if cleaned_url:
                        update_data(id, "url", cleaned_url)
                print(f"Finished updating URL for company {name}")

            linkedin_url = data['linkedin_url']
            if linkedin_url is None:
                print(f"Updating LinkedIn URL for company {name}")
                prompt = f"What is the LinkedIn URL of the company '{name}'?"
                linkedin_url = get_chatgpt_response(prompt, "linkedin_url")
                update_data(id, "linkedin_url", linkedin_url)

            gics_sub_industries = data['gics_sub_industries']
            if gics_sub_industries is None:
                print(f"Updating GICS sub-industry for company {name}")
                prompt = f"Can you classify '{name}' based on the Global Industry Classification Standard (GICS) classification under gics_sub_industries?"
                gics_sub_industries_response = get_chatgpt_response(
                    prompt, "gics_sub_industries")
                gics_sub_industries = find_and_validate_gics_value(
                    gics_sub_industries_response, gics_data, "gics_sub_industries")
                if gics_sub_industries is not None:
                    print(
                        f"GICS sub-industry response for company {name}: {gics_sub_industries}")

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
                print(f"Finished updating GICS data for company {name}")

        print("Data update complete.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
