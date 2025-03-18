
import pandas as pd
import re
from datetime import datetime
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Status categories
STATUS_CATEGORIES = {
    'Delivered': ["DEL_VERBAL", "DEL_ASR", "DEL_SIG", "DEL_OSNR"],
    'OFD Scans': ["ITR_OFD", "FEDEX_ACCEPTED", "PIC_CANPAR", "PURO_ACCEPTED"],
    'Return': ["EXC_BADADDRESS", "EXC_CONS_NA", "EXC_DMG", "EXC_MECHDELAY", "EXC_MISSING",
              "EXC_MISSORT", "EXC_NOACCESS", "EXC_NODELATTEMPT", "EXC_REC_NA", "EXC_RECCLOSED",
              "EXC_RECUNNDKL", "EXC_REFUSED", "EXC_UNSAFE", "EXC_WEATHER", "RET_PUR",
              "RET_TOR", "RET_WAR", "REC_TOR"],
    'Scansort': ["SCANSORT"],
    'Manifested': ['1'],
    'AJTM': ["AJTM"],
    'Lost in Transit': ["LOST_IN_TRANSIT"],
    'Pickup': ["PU01"]
}

def clean_city(city):
    """Clean city names by removing special characters and converting to title case"""
    city = re.sub(r'[^a-zA-Z0-9\s]', '', str(city))
    return city.title()

def categorize_status(status):
    """Categorize delivery status based on predefined categories"""
    for category, statuses in STATUS_CATEGORIES.items():
        if status in statuses:
            return category
    return "Other"

def categorize_service(route_code):
    """Categorize service based on route code prefix"""
    if isinstance(route_code, str):
        if route_code.startswith('YYZ-SD'):
            return 'Same Day'
        elif route_code.startswith('YYZ-'):
            return 'Next Day'
        elif route_code.startswith('YUL-'):
            return 'Montreal'
    return 'Other'

def calculate_rate(service, city):
    """Calculate delivery rate based on service type and city"""
    if service == 'Next Day':
        return 2.45 if city in ['Oakville', 'Burlington'] else 2.20
    elif service == 'Same Day':
        return 3.5
    elif service == 'Montreal':
        return 3.0
    return 0.0

def process_dispatch_data(df):
    """Process dispatch data and return categorized DataFrames"""
    # Clean column names
    df.columns = df.columns.str.replace(' ', '_')

    # Select and validate required columns
    selected_columns = ['Item_ID', 'Bill_To_Account_Number', 'Tracking_Number', 'Service',
                       'ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss)', 'Status', 'Status_Description',
                       'Route_Code', 'Delivery_Driver_Name', 'Delivery_Address', 'Delivery_City',
                       'Delivery_Province', 'Delivery_Postal_Code/ZIP', 'Delivery_Country',
                       'Latitude', 'Longitude', 'Client_Name']

    missing_columns = [col for col in selected_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    df_selected = df[selected_columns].copy()
    
    # Clean and transform data
    df_selected['Delivery_City'] = df_selected['Delivery_City'].apply(clean_city)
    df_selected = df_selected.rename(columns={'ScanCode_DateTime_(MM/DD/YYYY_HH:mm:ss)': 'Scan_Date'})
    df_selected['Scan_Date'] = pd.to_datetime(df_selected['Scan_Date'])
    df_selected['Date'] = df_selected['Scan_Date'].dt.date
    df_selected['Time'] = df_selected['Scan_Date'].dt.time
    df_selected['Updated_Status'] = df_selected['Status'].apply(categorize_status)

    # Create result DataFrame
    result_df = df_selected[df_selected['Updated_Status'] == 'OFD Scans'].groupby(
        ['Date', 'Delivery_Driver_Name', 'Route_Code']
    )['Item_ID'].nunique().reset_index()
    result_df = result_df.rename(columns={'Item_ID': 'Number_of_Packages'})

    # Add city information
    city_df = df_selected.groupby(['Date', 'Delivery_Driver_Name', 'Route_Code'])['Delivery_City'].first().reset_index()
    result_df = pd.merge(result_df, city_df, on=['Date', 'Delivery_Driver_Name', 'Route_Code'], how='left')

    # Add service categorization
    result_df['Service'] = result_df['Route_Code'].apply(categorize_service)

    # Add timing information
    for time_col, func in [('Start_Time', min), ('End_Time', max)]:
        result_df[time_col] = result_df.apply(
            lambda row: df_selected[
                (df_selected['Date'] == row['Date']) &
                (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
                (df_selected['Route_Code'] == row['Route_Code'])
            ]['Time'].apply(func),
            axis=1
        )

    # Calculate delivery metrics
    result_df['Delivered_No'] = result_df.apply(
        lambda row: df_selected[
            (df_selected['Date'] == row['Date']) &
            (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
            (df_selected['Route_Code'] == row['Route_Code']) &
            (df_selected['Updated_Status'] == 'Delivered')
        ]['Item_ID'].nunique(),
        axis=1
    )

    # Calculate confirmed returns
    result_df['Confirmed_Return'] = result_df.apply(
        lambda row: df_selected[
            (df_selected['Date'] == row['Date']) &
            (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
            (df_selected['Route_Code'] == row['Route_Code']) &
            (df_selected['Updated_Status'] == 'Return') &
            (~df_selected['Item_ID'].isin(
                df_selected[
                    (df_selected['Date'] == row['Date']) &
                    (df_selected['Delivery_Driver_Name'] == row['Delivery_Driver_Name']) &
                    (df_selected['Route_Code'] == row['Route_Code']) &
                    (df_selected['Updated_Status'] == 'Delivered')
                ]['Item_ID']
            ))
        ]['Item_ID'].nunique(),
        axis=1
    )

    # Calculate rates and amounts
    result_df['Rates'] = result_df.apply(lambda row: calculate_rate(row['Service'], row['Delivery_City']), axis=1)
    result_df['Amount_to_be_paid'] = result_df['Delivered_No'] * result_df['Rates']

    # Split into service-specific DataFrames
    return (
        result_df[result_df['Service'] == 'Next Day'],
        result_df[result_df['Service'] == 'Same Day'],
        result_df[result_df['Service'] == 'Montreal']
    )
