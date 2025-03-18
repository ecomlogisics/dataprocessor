import streamlit as st
import pandas as pd
import io
import re
from datetime import datetime
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Internal imports
def categorize_status(status):
    delivered_statuses = ["DEL_VERBAL", "DEL_ASR", "DEL_SIG", "DEL_OSNR"]
    ofd_scan_statuses = ["ITR_OFD", "FEDEX_ACCEPTED", "PIC_CANPAR", "PURO_ACCEPTED"]
    return_statuses = ["EXC_BADADDRESS", "EXC_CONS_NA", "EXC_DMG", "EXC_MECHDELAY", "EXC_MISSING",
                      "EXC_MISSORT", "EXC_NOACCESS", "EXC_NODELATTEMPT", "EXC_REC_NA", "EXC_RECCLOSED",
                      "EXC_RECUNNDKL", "EXC_REFUSED", "EXC_UNSAFE", "EXC_WEATHER", "RET_PUR",
                      "RET_TOR", "RET_WAR", "REC_TOR"]
    scansort_statuses = ["SCANSORT"]
    manifested_statuses = ['1']
    AJTM_statuses = ["AJTM"]
    lost_in_transit_statuses = ["LOST_IN_TRANSIT"]
    pickup_statuses = ["PU01"]

    if status in delivered_statuses:
        return "Delivered"
    elif status in ofd_scan_statuses:
        return "OFD Scans"
    elif status in return_statuses:
        return "Return"
    elif status in scansort_statuses:
        return "Scansort"
    elif status in lost_in_transit_statuses:
        return "Lost in Transit"
    elif status in pickup_statuses:
        return "Pickup"
    elif status in AJTM_statuses:
        return "AJTM"
    elif status in manifested_statuses:
        return "Manifested"
    else:
        return "Other"

def calculate_rate(row):
    service = row['Service']
    city = row['Delivery_City']
    if service == 'Next Day':
        if city in ['Oakville', 'Burlington']:
            return 2.45
        else:
            return 2.20
    elif service == 'Same Day':
        return 3.5
    elif service == 'Montreal':
        return 3
    else:
        return 0

def process_dispatch_data(df):
    """
    Process the dispatch data according to the business logic in Dispatch.py
    """
    # Clean column names
    df.columns = df.columns.str.replace(' ', '_')

    # Select required columns
    selected_columns = ['Item_ID', 'Bill_To_Account_Number', 'Tracking_Number', 'Service', 'ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss)',
                      'Status', 'Status_Description', 'Route_Code', 'Delivery_Driver_Name', 'Delivery_Address',
                      'Delivery_City', 'Delivery_Province', 'Delivery_Postal_Code/ZIP', 'Delivery_Country',
                      'Latitude', 'Longitude', 'Client_Name']

    # Check if all required columns exist in the uploaded file
    missing_columns = [col for col in selected_columns if col not in df.columns]
    if missing_columns:
        st.error(f"Missing required columns: {', '.join(missing_columns)}")
        st.error("Please make sure your CSV file contains all required columns.")
        return None

    df_selected = df[selected_columns]

    # Clean city names
    def clean_city(city):
        city = re.sub(r'[^a-zA-Z0-9\s]', '', str(city))
        city = city.title()
        return city

    df_selected['Delivery_City'] = df_selected['Delivery_City'].apply(clean_city)

    # Rename scan date column
    df_selected = df_selected.rename(columns={'ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss)': 'Scan_Date'})

    # Convert scan date to datetime
    df_selected['Scan_Date'] = pd.to_datetime(df_selected['Scan_Date'])

    # Extract date and time parts
    df_selected['Date'] = df_selected['Scan_Date'].dt.date
    df_selected['Time'] = df_selected['Scan_Date'].dt.time

    # Define status categories

    # Categorize status
    df_selected['Updated_Status'] = df_selected['Status'].apply(categorize_status)

    # Group by date, driver, and route, then count packages with 'OFD Scans' status
    result_df = df_selected[df_selected['Updated_Status'] == 'OFD Scans'].groupby(['Date', 'Delivery_Driver_Name', 'Route_Code'])['Item_ID'].nunique().reset_index()
    result_df = result_df.rename(columns={'Item_ID': 'Number_of_Packages'})

    # Get city information
    city_df = df_selected.groupby(['Date', 'Delivery_Driver_Name', 'Route_Code'])['Delivery_City'].first().reset_index()
    result_df = pd.merge(result_df, city_df, on=['Date', 'Delivery_Driver_Name', 'Route_Code'], how='left')

    # Categorize service based on route code
    def categorize_service(route_code):
        if isinstance(route_code, str):
            if route_code.startswith('YYZ-SD'):
                return 'Same Day'
            elif route_code.startswith('YYZ-'):
                return 'Next Day'
            elif route_code.startswith('YUL-'):
                return 'Montreal'
        return 'Other'

    result_df['Service'] = result_df['Route_Code'].apply(categorize_service)

    # Create Start_Time and End_Time columns
    result_df['Start_Time'] = result_df.apply(lambda row: df_selected[(df_selected['Date'] == row['Date']) &
                                                                   (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
                                                                   (df_selected['Route_Code'] == row['Route_Code'])]['Time'].min(), axis=1)

    result_df['End_Time'] = result_df.apply(lambda row: df_selected[(df_selected['Date'] == row['Date']) &
                                                                 (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
                                                                 (df_selected['Route_Code'] == row['Route_Code'])]['Time'].max(), axis=1)

    # Calculate Delivered_No
    result_df['Delivered_No'] = result_df.apply(lambda row: df_selected[(df_selected['Date'] == row['Date']) &
                                                                     (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
                                                                     (df_selected['Route_Code'] == row['Route_Code']) &
                                                                     (df_selected['Updated_Status'] == 'Delivered')]['Item_ID'].nunique(), axis=1)

    # Calculate Confirmed_Return
    result_df['Confirmed_Return'] = result_df.apply(lambda row: df_selected[
        (df_selected['Date'] == row['Date']) &
        (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
        (df_selected['Route_Code'] == row['Route_Code']) &
        (df_selected['Updated_Status'] == 'Return') &
        (~df_selected['Item_ID'].isin(df_selected[
            (df_selected['Date'] == row['Date']) &
            (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
            (df_selected['Route_Code'] == row['Route_Code']) &
            (df_selected['Updated_Status'] == 'Delivered')
        ]['Item_ID']))
    ]['Item_ID'].nunique(), axis=1)

    # Calculate Rates
    result_df['Rates'] = result_df.apply(calculate_rate, axis=1)

    # Calculate Amount_to_be_paid
    result_df['Amount_to_be_paid'] = result_df['Delivered_No'] * result_df['Rates']

    # Create separate DataFrames for different services
    next_day_df = result_df[result_df['Service'] == 'Next Day']
    same_day_df = result_df[result_df['Service'] == 'Same Day']
    montreal_df = result_df[result_df['Service'] == 'Montreal']

    return next_day_df, same_day_df, montreal_df

def create_excel_report(next_day_df, same_day_df, montreal_df):
    """
    Create an Excel file with multiple sheets from the processed data
    """
    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        # Write each DataFrame to a different sheet
        next_day_df.to_excel(writer, sheet_name='Next_Day', index=False)
        same_day_df.to_excel(writer, sheet_name='Same_Day', index=False)
        montreal_df.to_excel(writer, sheet_name='Montreal', index=False)

        # Get the workbook and worksheet objects
        workbook = writer.book

        # Add formatting for each sheet
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D3D3D3',
            'border': 1
        })

        for sheet_name, df in [('Next_Day', next_day_df), ('Same_Day', same_day_df), ('Montreal', montreal_df)]:
            worksheet = writer.sheets[sheet_name]

            # Apply header format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            # Auto-adjust columns width
            for column in df:
                column_length = max(df[column].astype(str).apply(len).max(), len(column))
                col_idx = df.columns.get_loc(column)
                worksheet.set_column(col_idx, col_idx, column_length + 2)

    return buffer

def main():
    # App header
    st.title("Ecom Dispatch Report")

    # Add description
    st.write("""
    Upload your CSV file (up to 500MB) and get a formatted Excel report in return.
    The Excel report will include formatted headers and auto-adjusted column widths.
    """)

    # File upload with increased size limit
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        try:
            # Show processing status
            with st.spinner('Reading CSV file...'):
                # Read CSV
                df = pd.read_csv(uploaded_file)

                # Show preview of the data
                st.subheader("Preview of uploaded data")
                st.dataframe(df.head())

                # Show basic statistics
                st.subheader("Dataset Information")
                st.write(f"Number of rows: {df.shape[0]}")
                st.write(f"Number of columns: {df.shape[1]}")

                # Process button
                if st.button("Generate Dispatch Report"):
                    with st.spinner('Processing data and generating Excel report...'):
                        # Process the data according to the business logic
                        result = process_dispatch_data(df)

                        if result is not None:
                            next_day_df, same_day_df, montreal_df = result

                            # Create Excel report
                            excel_buffer = create_excel_report(next_day_df, same_day_df, montreal_df)

                            # Generate timestamp for filename
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                            # Create download button
                            st.download_button(
                                label="Download Excel Report",
                                data=excel_buffer.getvalue(),
                                file_name=f"dispatch_report_{timestamp}.xlsx",
                                mime="application/vnd.ms-excel"
                            )

                            # Show summary statistics
                            st.success("Excel report generated successfully!")

                            st.subheader("Report Summary")
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Next Day Deliveries", len(next_day_df))
                            with col2:
                                st.metric("Same Day Deliveries", len(same_day_df))
                            with col3:
                                st.metric("Montreal Deliveries", len(montreal_df))

                            # Show preview of each sheet
                            tab1, tab2, tab3 = st.tabs(["Next Day", "Same Day", "Montreal"])
                            with tab1:
                                st.dataframe(next_day_df)
                            with tab2:
                                st.dataframe(same_day_df)
                            with tab3:
                                st.dataframe(montreal_df)

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.write("Please make sure your CSV file is properly formatted and try again.")

if __name__ == "__main__":
    main()