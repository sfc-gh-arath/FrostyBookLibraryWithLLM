# FrostyBookLibraryWithLLM
A streamlit in Snowflake App that downloads books from www.gutenberg.org and uses LLM to analyze it.


### FrostyLibrary Deployment
You can deploy FrostyTracking in your Snowflake account and customize this project in your environment by following these 5 steps.
1) Download the frostylibrary_llm_sis.py and logo_frostylibrary.png and setup.sqlfiles from the GitHub Repository.
2) Run the setup_llm.sql in a Snowsight worksheet.
3) Create a new Streamlit app within your Snowflake account via Snowsight -> "Projects" -> "Streamlit" -> "+ Streamlit App" (define the WH to be used, location, and your application name).
4) Paste the code from the frostylibrary_llm_sis.py file into your new Streamlit app and import the plotly package from the "Packages" menu.
5) Optional: Upload the logo_frostylibrary.png image to the Streamlit application stage in Snowflake (recommended to make it fancy!)

## Based on the original blog of Matteo-Consoli "https://medium.com/snowflake/frostylibrary-a-snowflake-cortex-llm-demo-bff6d8c2f736"
