-- Execute the code below as-is in a Snowsight worksheet.
-- Create objects (for simplicity, in this demo we'll use accountadmin role)

/****
Setup a database to host all the data, functions for Frosty Library
***/

CREATE DATABASE IF NOT EXISTS FROSTYLIBRARY;
CREATE SCHEMA IF NOT EXISTS FROSTYLIBRARY.HOL_LLM;
USE SCHEMA FROSTYLIBRARY.HOL_LLM;
CREATE WAREHOUSE IF NOT EXISTS FROSTYLIBRARY_WH
    WAREHOUSE_SIZE = 'XSmall' 
    AUTO_SUSPEND=60 
    AUTO_RESUME=True;

-- Table creating (staging and archive)
CREATE OR REPLACE TABLE frosty_library (
   books_output VARIANT
);
CREATE TABLE frosty_library_archive_summary_table (
    book_url VARCHAR,
    book_summary VARCHAR,
    book_author VARCHAR,
    book_title VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

/*****
Setup Network rulles to access external sites
For demo we will just use www.gutenberg.org for public domain books
****/

CREATE OR REPLACE NETWORK RULE network_get_book MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('www.gutenberg.org');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION integration_get_book
        ALLOWED_NETWORK_RULES = (network_get_book)
        ALLOWED_AUTHENTICATION_SECRETS = ()
        ENABLED = TRUE;

/*****
Let's create a function to import books from internet - in the example below we use gutenberg.org)
*****/
        
CREATE OR REPLACE FUNCTION import_book(parameter STRING, book_title STRING, book_author STRING)
  RETURNS VARIANT
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.10
  HANDLER = 'API_get_book'
  EXTERNAL_ACCESS_INTEGRATIONS = (integration_get_book)
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  AS
$$
import requests

def API_get_book(url, book_title, book_author):  
    try:
        url
    except NameError:
        url = ''

    apiURL = '{param}'.format(param=url)
    response = requests.get(apiURL)
    if response.status_code == 200:
        content = response.content.decode('utf-8')
        chunks = [content[i:i+16384] for i in range(0, len(content), 16384)]
        return [{'book_title': book_title, 'book_author': book_author,  'url': url, 'sequence': i + 1, 'content': chunk} for i, chunk in enumerate(chunks)]
    else:
        return [{'error': apiURL + ' Error Code: ' + str(response.status_code) + ' Message: ' + response.text}]
$$;

/***
Let's create a View exposing flattened data from the staging folder.
****/

CREATE OR REPLACE VIEW frosty_library_flattened AS  
SELECT 
f.value:book_title as book_title,
f.value:book_author as book_author,
f.value:url as url_book_id,
f.value:sequence as content_sequence,
f.value:content as content
FROM
    frosty_library,
   lateral flatten(input => books_output, path => '') f;
/*****
Lets create a python function to parse the content we scraped from the website
****/
CREATE OR REPLACE FUNCTION HTML_STRIP("ARG1" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('beautifulsoup4')
HANDLER = 'html_strip'
AS
$$
from bs4 import BeautifulSoup
def html_strip(text):
    soup = BeautifulSoup(text, 'html.parser')
    return soup.get_text(" ", strip=True)
$$
;

/****
Let's create a table to vectorize the chunked data
****/
CREATE OR REPLACE TABLE frosty_library_flattened_vectors AS 
SELECT 
    *,
    snowflake.cortex.embed_text('e5-base-v2', HTML_STRIP(content)) as content_chunk_vector
FROM 
    frosty_library_flattened;

/*****
let's create a function that can find the nearest chunk of data that presents the prompt and use cortex to get the answer
****/
CREATE OR REPLACE FUNCTION BOOK_SERACH_LLM(prompt string,title STRING, author STRING)
RETURNS TABLE (response string,CONTENT_SEQUENCE variant,score float)
AS
    $$
    WITH best_match_chunk AS (
        SELECT
            v.BOOK_TITLE,
            v.BOOK_AUTHOR,
            v.URL_BOOK_ID,
            v.CONTENT_SEQUENCE,
            HTML_STRIP(v.CONTENT) as CONTENT,
            VECTOR_COSINE_DISTANCE(v.content_chunk_vector, snowflake.cortex.embed_text('e5-base-v2', prompt)) AS score
        FROM 
            frosty_library_flattened_vectors v
            where BOOK_TITLE=title
            and BOOK_AUTHOR=author
            --and score > '0.8'
        ORDER BY 
            score DESC
        LIMIT 10
    )
    SELECT 
            SNOWFLAKE.cortex.COMPLETE('mistral-large',CONCAT('Answer this question: ', prompt, '\n\nUsing this book text: ', CONTENT)) AS response,
            CONTENT_SEQUENCE,
            score
            FROM
            best_match_chunk
    $$;    

/****
Lets find the best answers and sumamrize it
*****/
CREATE OR REPLACE FUNCTION BOOK_SERACH_LLM_SUMMARIZE(prompt string,title STRING, author STRING)
RETURNS TABLE (response string)
AS
    $$
    WITH best_match_chunk AS (
        SELECT
            v.BOOK_TITLE,
            v.BOOK_AUTHOR,
            v.URL_BOOK_ID,
            v.CONTENT_SEQUENCE,
            HTML_STRIP(v.CONTENT) as CONTENT,
            VECTOR_COSINE_DISTANCE(v.content_chunk_vector, snowflake.cortex.embed_text('e5-base-v2', prompt)) AS score
        FROM 
            frosty_library_flattened_vectors v
            where BOOK_TITLE=title
            and BOOK_AUTHOR=author
        ORDER BY 
            score DESC
        LIMIT 10
    ),
    best_answers AS (
    SELECT 
        ARRAY_TO_STRING(ARRAY_AGG(response),' ,') AS json_payload
    FROM 
        (SELECT 
            SNOWFLAKE.cortex.COMPLETE('mistral-large',CONCAT('Answer this question: ', prompt, '\n\nUsing this book text: ', CONTENT)) AS response,
            BOOK_TITLE,
            BOOK_AUTHOR,
            URL_BOOK_ID,
            CONTENT_SEQUENCE,
            score
            FROM
            best_match_chunk)
        )
    SELECT snowflake.cortex.summarize(json_payload) as RESPONSE
    from best_answers
    $$;    
    
   
--test code
select * from frosty_library;


delete from frosty_library
where books_output like '%"error":%';

SELECT DISTINCT url_book_id, CONCAT(book_author, ' - ', book_title)
            FROM frosty_library_flattened;  

SELECT *
FROM frosty_library_flattened
limit 10; 

select distinct book_title,book_author 
FROM frosty_library_flattened;

SELECT *
FROM frosty_library_flattened
where book_author = 'Kabir'
limit 10;  

SELECT *
FROM frosty_library_flattened_vectors
limit 10;  


SELECT HTML_STRIP(content),*
FROM frosty_library_flattened
limit 10;  

-- Test the LLM:
SET prompt = 'What are the quotes about immoral books?';
SELECT * FROM TABLE(BOOK_SERACH_LLM($prompt,'The Picture of Dorian Gray','Oscar Wilde'));
SELECT RESPONSE FROM TABLE(BOOK_SERACH_LLM('What are the quotes about morality in this book ?','The Picture of Dorian Gray','Oscar Wilde'));

SET prompt = 'What are the quotes about immorality and books?';
SET title = 'The Picture of Dorian Gray';
SET author = 'Oscar Wilde';
SELECT * FROM TABLE(BOOK_SERACH_LLM($prompt,$title,$author));

SELECT * FROM TABLE(BOOK_SERACH_LLM('what are the quotes about beautiful things?','The Picture of Dorian Gray','Oscar Wilde'));

--need to fix the book titles
-- lets create a UDF that helps in updating the desired key within the JSON string

update frosty_library
set books_output = replace(books_output,'"book_title": "Songs of Kabir by Kabir"','"book_title":"Songs of Kabir"')
;

create or replace function json_update_book_title ("v" variant, "NewValue" string)
returns variant
language javascript
as
$$
   v:book_title = NewValue;
   return v;
$$;

-- lets see how the change works without updating the row
select to_variant(books_output[0])
from frosty_library;

select to_variant(books_output[0]):book_title
from frosty_library;

select (to_variant(books_output[0]):book_title='new value')
from frosty_library;


select json_update_book_title(to_variant(books_output[0]), 'Songs of Kabir') 
from frosty_library;

