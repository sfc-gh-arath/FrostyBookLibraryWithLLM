import streamlit as st
import base64
import os
import json
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session


### --------------------------- ###
### Header & Config             ###  
### --------------------------- ###

# Set page title, icon, description
st.set_page_config(
    page_title="FrostyLibrary - (www.gutenberg.org)",
    layout="wide",
    initial_sidebar_state="expanded",
)

### ---------------------------- ###
### Sidebar - Configurations     ### 
### ---------------------------- ###

#Logo
image_name = 'logo_frostylibrary.png'
mime_type = image_name.split('.')[-1:][0].lower() 
if os.path.isfile(image_name):
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.sidebar.image(image_string)
else:
    st.sidebar.write("Logo not uploaded in Streamlit App Stage")

# Streamlit app
selected_page = st.sidebar.selectbox('Select Dashboard',['Import Book', 'Generate Book Summary', 'Get Book Summary from Archive','Ask questions'])
#Credits
st.sidebar.text("Based on Matteo Consoli blog ")
st.sidebar.text("https://medium.com/snowflake/frostylibrary-a-snowflake-cortex-llm-demo-bff6d8c2f736")

### --------------------------- ###
### Backend Functions           ###  
### --------------------------- ###

def get_snowflake_connection():
        return get_active_session()
st.session_state.snowflake_connection = get_active_session()

# Function to fetch data from Snowflake
def fetch_data(query):
    conn = get_snowflake_connection()
    return conn.sql(query).collect();

def import_book(url, title, author):
    # Execute the INSERT INTO command
    insert_query = f"INSERT INTO frosty_library SELECT import_book('{url}', '{title}', '{author}')"
    query_output = fetch_data(insert_query)
    # Confirm the upload
    st.success('Book uploaded successfully!')

# Function to summarize content for a given book ID
def summarize_flattened_view(book_id):
    if book_id != '':
        # Execute the query to fetch content for the given book_id
        query_content = f"SELECT content FROM frosty_library_flattened WHERE url_book_id = '{book_id}' ORDER BY content_sequence ASC"
        query_output = fetch_data(query_content)
        query_metadata = f"SELECT book_title, book_author FROM frosty_library_flattened WHERE url_book_id = '{book_id}' LIMIT 1"
        query_output_metadata = fetch_data(query_metadata)
        book_title, boook_author = query_output_metadata[0]
        # Initialize an empty list to store the summaries
        summaries = []

        # Iterate over the rows and call the SNOWFLAKE.CORTEX.SUMMARIZE function
        for row in query_output:
            content = row[0].replace("'","'\'")
            # Call SNOWFLAKE.CORTEX.SUMMARIZE function for each content
            summarize_query = f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{content}')"
            summary_result = fetch_data(summarize_query)
            summary_value = summary_result[0][0] if summary_result else None
            if summary_value is not None:
                summaries.append(summary_value)
            else:
        # Handle the case where summary_result is None or empty
                summaries.append('No summary available')
        # Concatenate all summaries into a single object
        concatenated_object = " ".join(summaries)
        concatenated_object = concatenated_object.replace('\'', '\\''')

        # Finally, run the summarize function over the concatenated object and store it to the archive table
        summarize_final_query = f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{concatenated_object}')"
        final_summary_result = fetch_data(summarize_final_query)
        conn = get_snowflake_connection()
        query_insert_archive = """
                INSERT INTO frosty_library_archive_summary_table (book_url, book_summary, book_author, book_title)
                VALUES (?, ?, ?, ?)
            """
        conn.sql(query_insert_archive, (book_id, final_summary_result[0][0], boook_author, book_title)).collect()
        return final_summary_result[0][0]
    else:
        return "No Book Selected"

def get_distinct_urls():
    # Execute the query to fetch distinct URLs along with author and title
    query = """
            SELECT DISTINCT url_book_id, CONCAT(book_author, ' - ', book_title)
            FROM frosty_library_flattened
    """
    results = fetch_data(query)
    # Strip double quotes from the URLs
    cleaned_results = [(url.strip('"'), label) for url, label in results]
    return cleaned_results

def get_distinct_urls_in_archive():
    # Execute the query to fetch distinct URLs along with author and title
    query = """
            SELECT DISTINCT book_url, CONCAT(book_author, ' - ', book_title), book_summary
            FROM frosty_library_archive_summary_table 
    """
    results = fetch_data(query)
    # Strip double quotes from the URLs
    cleaned_results = [(url.strip('"'), author_title, summary) for url, author_title, summary in results]
    return cleaned_results  

def get_distinct_book_detail_in_archive():
    # Execute the query to fetch distinct URLs along with author and title
    query = """
            SELECT DISTINCT book_url, book_author, book_title, book_summary
            FROM frosty_library_archive_summary_table 
    """
    results = fetch_data(query)
    # Strip double quotes from the URLs
    cleaned_results = [(url.strip('"'), author,title, summary) for url, author, title, summary in results]
    return cleaned_results  

def get_book_review(author,title):
    title = title.strip('"')
    query = f"""
    select name, author, summary, COMMUNITY_REVIEWS
    FROM GOODREADS_BOOKS.PUBLIC.GOODREADSBOOK
    where name  like '{title}'
    and author like '%{author}%'
    """
    results = fetch_data(query)
    return results  
def get_cortex_responses(prompt,author,title):
    # Execute the query to fetch distinct URLs along with author and title
    title = title.strip('"')
    author = author.strip('"')
    query = f"SELECT * FROM TABLE(BOOK_SERACH_LLM('{prompt}','{title}','{author}'));"
    #st.write(query)
    try:
        # Execute the query and fetch the results
        results = fetch_data(query)
        return results  
    except Exception as e:
        return f"Error executing query: {e}"


### ---------------------------- ###
### Main UI - Views              ### 
### ---------------------------- ###

st.title('FrostyLibrary')
st.subheader('Tool For Lazy Readers powered by :snowflake: Cortex LLM')

# Display selected insights
if 'Import Book' in selected_page:
    # Text inputs for book title, author, and URL
    st.markdown("Please find a book from www.gutenberg.org and enter the details below. We will import the text of the book")
    title = st.text_input('Enter Book Title:')
    author = st.text_input('Enter Book Author:')
    url = st.text_input('Enter Book URL:')
    # Button to import the book
    if st.button('Import Book'):
        # Validate inputs
        if not (title and author and url):
            st.error('Please enter book title, author, and URL.')
        else:
            # Call the import_book function
            with st.spinner(text="Hey, I'm importing this book. I'm splitting it in small chunks as well!"):
                    import_book(url, title, author)

if 'Generate Book Summary' in selected_page:
    # Retrieve distinct URLs along with author and title
    distinct_urls = get_distinct_urls()
    url_options = {url: label for url, label in distinct_urls}
    
    # Dropdown menu for selecting book URL
    selected_url = st.selectbox('Select a Book from your Library:', options=list(url_options.keys()), format_func=lambda x: url_options[x]) 
    if st.button('Generate Summary'):
    # Validate book_id
        if not selected_url:
            st.error('Please select a book.')
        else:
            # Retrieve and display summary
            try:
                with st.spinner(text="Hey, give me time! I'm reading this book faster than you!"):
                    summary = summarize_flattened_view(selected_url)
                    st.text_area('Summary', value=summary, height=400)
            except Exception as e:
                st.error(f'Error occurred: {str(e)}')
        
if 'Get Book Summary from Archive' in selected_page:
    # Retrieve distinct URLs along with author and title and summary
    distinct_urls = get_distinct_book_detail_in_archive()
    url_options = {url: (author,title, summary) for url, author,title, summary in distinct_urls}
    
    # Dropdown menu for selecting book URL
    selected_url = st.selectbox('Select a Summary from your Library:', options=list(url_options.keys()), format_func=lambda x: '-'.join([url_options[x][0],url_options[x][1]])) 

    # Display the selected summary
    if selected_url in url_options:
        author,title, book_summary = url_options[selected_url]
        st.text_area('Summary', value=book_summary, height=300)
        #Display GoodReads reviews
        st.markdown("Good Reads Review (collected through Snowflake Marketplace Bright Data)")
        reviews = get_book_review(author,title)
        if reviews :
            st.text_area('Good Reads Summary', value=reviews[0][2], height=300)
            ratings_json = json.loads(reviews[0][3])
            # st.write(ratings_json)
            st.write('Good Reads User Rating')
            one_star= ratings_json["1_stars"]["reviews_num"]
            one_star_pct= ratings_json["1_stars"]["reviews_percentage"]
            two_star= ratings_json["2_stars"]["reviews_num"]
            two_star_pct= ratings_json["2_stars"]["reviews_percentage"]
            three_star= ratings_json["3_stars"]["reviews_num"]
            three_star_pct= ratings_json["3_stars"]["reviews_percentage"]
            four_star= ratings_json["4_stars"]["reviews_num"]
            four_star_pct= ratings_json["4_stars"]["reviews_percentage"]
            five_star= ratings_json["5_stars"]["reviews_num"]
            five_star_pct= ratings_json["5_stars"]["reviews_percentage"]
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric(f'⭐', f'{one_star}', f'{one_star_pct}%')
            col2.metric(f'⭐⭐', f'{two_star}',f'{two_star_pct}%')
            col3.metric(f'⭐⭐⭐', f'{three_star}',f'{three_star_pct}%')
            col4.metric(f'⭐⭐⭐⭐', f'{four_star}',f'{four_star_pct}%')
            col5.metric(f'⭐⭐⭐⭐⭐', f'{five_star}',f'{five_star_pct}%')
            
        else:
            st.write('No reviews available in Goodreads')

    else:
        st.error('No summary found for the selected URL.')

if 'Ask questions' in selected_page:
    # Retrieve distinct URLs along with author and title and summary
    # distinct_urls = get_distinct_urls_in_archive()
    # url_options = {url: (author_title, summary) for url, author_title, summary in distinct_urls}

    distinct_urls = get_distinct_book_detail_in_archive()
    url_options = {url: (author,title, summary) for url, author,title, summary in distinct_urls}

    
    # Dropdown menu for selecting book URL
    selected_url = st.selectbox('Select a Summary from your Library:', options=list(url_options.keys()), format_func=lambda x: '-'.join([url_options[x][0],url_options[x][1]])) 

    #selected_url = st.selectbox('Select a Book from your Library:', options=list(url_options.keys()), format_func=lambda x: url_options[x][0]) 

    # Display the selected summary
    if selected_url in url_options:
        author,title, book_summary = url_options[selected_url]
        if prompt := st.chat_input("Enter your Question"):
            # Display teh question in chat message container
            with st.chat_message("user"):
                st.markdown(prompt)
            # Display assistant response in chat message container
            with st.chat_message("assistant"):
                #response = st.write_stream(response_generator())
                response = get_cortex_responses(prompt,author,title)
                st.write(response)
                #st.text_area('response', value=response[0][0], height=400)
    else:
        st.error('No summary found for the selected URL.')


