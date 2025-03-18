
import streamlit as st
import pandas as pd
from data_processor import process_dispatch_data, categorize_status, calculate_rate
import io
from datetime import datetime

def create_excel_report(next_day_df, same_day_df, montreal_df):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        for name, df in [('Next_Day', next_day_df), ('Same_Day', same_day_df), ('Montreal', montreal_df)]:
            df.to_excel(writer, sheet_name=name, index=False)
            worksheet = writer.sheets[name]
            for idx, col in enumerate(df.columns):
                worksheet.set_column(idx, idx, len(str(col)) + 2)
    return buffer

def main():
    st.title("Ecom Dispatch Report")
    st.write("Upload your CSV file (up to 500MB) and get a formatted Excel report.")
    
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            st.dataframe(df.head())
            
            if st.button("Generate Dispatch Report"):
                with st.spinner('Processing...'):
                    processed_df = process_dispatch_data(df)
                    # Rest of your processing logic...
                    
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    "Download Report",
                    data=create_excel_report(next_day_df, same_day_df, montreal_df).getvalue(),
                    file_name=f"dispatch_report_{timestamp}.xlsx",
                    mime="application/vnd.ms-excel"
                )
                
        except Exception as e:
            st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
