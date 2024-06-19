# Import required Libraries
import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def main():
    cn = init()
    if cn:
        schema_list = get_obj_list(cn, "SHOW SCHEMAS LIKE 'WORLD_%'")
        schema = st.radio("Select Schema", schema_list)
        table_list= get_obj_list(cn, f"SHOW TABLES IN SCHEMA {schema}")
        table = st.radio("Select Table", table_list)

        uploaded_file = st.file_uploader(label=f"Select file to ingest into {schema}.{table}", type="csv", accept_multiple_files=False)
        if uploaded_file is not None:
            df_csv = pd.read_csv(uploaded_file)
            success, nchunks, nrows, _ = write_pandas( conn=cn
                , df = df_csv
                , database = "FROSTY_FRIDAY"
                , schema = schema
                , table_name = table
                , quote_identifiers = False)
            if success:
                st.write(f"Your upload was a success. You uploaded {nrows} rows.")
            else:
                st.write("Your upload was a error.")
        else:
            st.write("Awaiting file to upload...")
        #session.close()

@st.cache_resource
def init():
    st.title("Manual CSV File to Snowflake Table Uploader")
    st.sidebar.image(r"C:\Users\Naoko.Mimura\Documents\30_development\Striamlit\frostyfriday\week_12\uploaddata\osarusan.png")
    sidebar_text = '''# Instructions:
        - Select the schema from the available.
        - Then select the table which will automatically update to reflect your schema choice.
        - Check that the table corresponds to that which you want to ingest into.
        - Select the file you want to ingest.
        - You should see an upload success message detailing how many rows were ingested.
    '''
    for line in sidebar_text.split('\n') :
        st.sidebar.write(line)
    cn = snowflake.connector.connect(**st.secrets["snowflake"])
    return cn

@st.cache_data()
def get_obj_list(_cn,command):
    o_list = _cn.cursor().execute(command).fetchall()
    name_list = pd.DataFrame(o_list)[1]
    return name_list 

if __name__ == "__main__":
    main()