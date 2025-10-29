import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd
from datetime import datetime
import uuid
import json
from io import BytesIO
import base64
import os
import tempfile

# Configuration
st.set_page_config(layout="wide", page_title="Invoice Processing System")

# Initialize session
session = get_active_session()

# Styling
st.markdown("""
    <style>
    /* Reduce overall font sizes */
    html, body, [class*="css"] {
        font-size: 12px !important;
    }
    
    /* Title and heading sizes */
    h1 {
        font-size: 24px !important;
        margin-bottom: 10px !important;
    }
    
    h2 {
        font-size: 16px !important;
        margin-bottom: 8px !important;
        margin-top: 8px !important;
    }
    
    h3 {
        font-size: 14px !important;
        margin-bottom: 6px !important;
        margin-top: 6px !important;
    }
    
    /* Labels and text */
    label {
        font-size: 12px !important;
    }
    
    p {
        font-size: 12px !important;
        margin-bottom: 6px !important;
    }
    
    /* Input fields */
    input, textarea, select {
        font-size: 11px !important;
    }
    
    /* Buttons */
    button {
        font-size: 11px !important;
        padding: 2px 4px !important;
        height: 32px !important;
        min-height: 32px !important;
    }
    
    /* Container padding */
    .header-box {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 8px;
        margin-bottom: 8px;
    }
    
    .editable-field {
        margin-bottom: 6px;
    }
    
    /* Reduce spacing in containers */
    [data-testid="column"] {
        gap: 8px !important;
    }
    
    /* Alert/message boxes */
    [data-testid="stAlert"] {
        font-size: 12px !important;
        padding: 10px !important;
    }
    
    /* Divider */
    hr {
        margin: 8px 0 !important;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🏢 Invoice Processing System")

# Initialize session state
if "extracted_data" not in st.session_state:
    st.session_state.extracted_data = None
if "header_df" not in st.session_state:
    st.session_state.header_df = None
if "detail_df" not in st.session_state:
    st.session_state.detail_df = None
if "pdf_content" not in st.session_state:
    st.session_state.pdf_content = None
if "file_path" not in st.session_state:
    st.session_state.file_path = None
if "header_editable" not in st.session_state:
    st.session_state.header_editable = {}
if "detail_editable" not in st.session_state:
    st.session_state.detail_editable = {}
if "ai_extract_completed" not in st.session_state:
    st.session_state.ai_extract_completed = False
if "current_file_id" not in st.session_state:
    st.session_state.current_file_id = None
if "header_changes" not in st.session_state:
    st.session_state.header_changes = {}
if "detail_changes" not in st.session_state:
    st.session_state.detail_changes = {}

# ====================== HELPER FUNCTIONS ======================

def upload_file_to_stage(uploaded_file):
    """Upload file to Snowflake internal stage"""
    try:
        unique_id = str(uuid.uuid4())[:8]
        file_ext = uploaded_file.name.split('.')[-1]
        file_name_only = uploaded_file.name.rsplit('.', 1)[0]
        current_date = datetime.now().strftime("%m-%d-%Y")
        
        filename = f"{file_name_only}_{unique_id}.{file_ext}"
        stage_path = f"invoices/{current_date}/{filename}"
        full_stage_path = f"@<DB_NAME>.<SCHEMA>.INVOICE_UPLOADS/{stage_path}"
        
        # Read file bytes
        file_bytes = BytesIO(uploaded_file.getvalue())
        
        # Put file to stage
        result = session.file.put_stream(
            file_bytes,
            stage_location=f"@<DB_NAME>.<SCHEMA>.INVOICE_UPLOADS/{stage_path}",
            auto_compress=False
        )
        
        # Verify file was uploaded
        st.success(f"✅ File uploaded to stage: {full_stage_path}")
        
        return full_stage_path
    except Exception as e:
        st.error(f"Error uploading file to stage: {str(e)}")
        return None

def extract_invoice_data(file_path):
    """Extract invoice data using Snowflake AI_EXTRACT"""
    try:
        query = f"""
        SELECT AI_EXTRACT(
            file => TO_FILE('@<DB_NAME>.<SCHEMA>.INVOICE_UPLOADS', '{file_path}'),
            responseFormat => {{
                'schema': {{
                    'type': 'object',
                    'properties': {{
                        'VendorName': {{
                            'type': 'string',
                            'description': 'The name of the company sending the invoice'
                        }},
                        'InvoiceNo': {{
                            'type': 'string',
                            'description': 'The unique invoice identifier number'
                        }},
                        'PurchaseOrderNo': {{
                            'type': 'string',
                            'description': 'The unique optional purchase order number'
                        }},
                        'InvoiceDate': {{
                            'type': 'string',
                            'description': 'The date the invoice was issued (e.g., YYYY-MM-DD)'
                        }},
                        'InvoiceSubtotal': {{
                            'type': 'string',
                            'description': 'The total amount before taxes and fees'
                        }},
                        'InvoiceTaxAmount': {{
                            'type': 'string',
                            'description': 'The total amount of tax charged on the invoice'
                        }},
                        'InvoiceTotal': {{
                            'type': 'string',
                            'description': 'The final total amount due, including taxes and fees'
                        }},
                        'DepositCreditAmount': {{
                            'type': 'string',
                            'description': 'The deposit or credit amount applied to the invoice (optional)'
                        }},
                        'Quantities': {{
                            'type': 'array',
                            'description': 'An array of all quantities for each line item',
                            'items': {{'type': 'string'}}
                        }},
                        'ItemDescriptions': {{
                            'type': 'array',
                            'description': 'An array of all descriptions for each line item',
                            'items': {{'type': 'string'}}
                        }},
                        'LineTotals': {{
                            'type': 'array',
                            'description': 'An array of all total prices for each line item',
                            'items': {{'type': 'string'}}
                        }}
                    }}
                }}
            }}
        ) as extracted_data;
        """
        
        result = session.sql(query).collect()
        if result:
            extracted_json = result[0][0]
            return json.loads(extracted_json)
        return None
    except Exception as e:
        st.error(f"Error extracting data: {str(e)}")
        return None

def split_data_into_dataframes(extracted_data):
    """Split extracted data into header and detail dataframes"""
    try:
        response = extracted_data.get("response", {})
        
        # Header data
        header_data = {
            "VendorName": response.get("VendorName", ""),
            "InvoiceNo": response.get("InvoiceNo", ""),
            "PurchaseOrderNo": response.get("PurchaseOrderNo", ""),
            "InvoiceDate": response.get("InvoiceDate", ""),
            "InvoiceSubtotal": response.get("InvoiceSubtotal", "0"),
            "InvoiceTaxAmount": response.get("InvoiceTaxAmount", "0"),
            "InvoiceTotal": response.get("InvoiceTotal", "0"),
            "DepositCreditAmount": response.get("DepositCreditAmount", "0"),
        }
        
        header_df = pd.DataFrame([header_data])
        
        # Detail data
        quantities = response.get("Quantities", [])
        descriptions = response.get("ItemDescriptions", [])
        line_totals = response.get("LineTotals", [])
        
        # Ensure all arrays are same length
        max_len = max(len(quantities), len(descriptions), len(line_totals))
        quantities.extend([""] * (max_len - len(quantities)))
        descriptions.extend([""] * (max_len - len(descriptions)))
        line_totals.extend([""] * (max_len - len(line_totals)))
        
        detail_data = {
            "LineNumber": list(range(1, max_len + 1)),
            "Description": descriptions,
            "Quantity": quantities,
            "LineTotal": line_totals,
        }
        
        detail_df = pd.DataFrame(detail_data)
        
        return header_df, detail_df
    except Exception as e:
        st.error(f"Error splitting data: {str(e)}")
        return None, None

def display_pdf_from_stage(pdf_stage_path):
    """Display PDF from Snowflake stage using st.pdf"""
    try:
        # Properly quote the stage path for SQL
        quoted_stage_path = pdf_stage_path.replace("'", "''")
        
        # Extract filename safely
        filename = pdf_stage_path.split('/')[-1]
        
        # Create temp directory for the file
        temp_dir = tempfile.gettempdir()
        pdf_dir = os.path.join(temp_dir, "streamlit_invoices")
        os.makedirs(pdf_dir, exist_ok=True)
        local_path = os.path.join(pdf_dir, filename)
        
        # Get the file from stage with proper quoting
        get_command = f"GET '{quoted_stage_path}' 'file://{pdf_dir}/' OVERWRITE=TRUE"
        result = session.sql(get_command).collect()
        
        # Display PDF using st.pdf with file path
        st.pdf(local_path)
        
        # Also provide download option
        with open(local_path, "rb") as f:
            st.download_button(
                label="📥 Download PDF",
                data=f.read(),
                file_name=filename,
                mime="application/pdf"
            )
    except Exception as e:
        st.error(f"Failed to load PDF: {e}")
        st.info("Make sure the file exists in your stage.")

def display_pdf(file_bytes, file_type):
    """Display PDF or Image file"""
    try:
        if file_type.lower() == "pdf":
            st.write("📄 PDF Preview")
            
            # Save PDF locally temporarily
            temp_dir = tempfile.gettempdir()
            pdf_dir = os.path.join(temp_dir, "streamlit_invoices")
            os.makedirs(pdf_dir, exist_ok=True)
            
            # Create unique filename
            unique_id = str(uuid.uuid4())[:8]
            temp_pdf_path = os.path.join(pdf_dir, f"invoice_{unique_id}.pdf")
            
            # Write PDF to temp file
            with open(temp_pdf_path, "wb") as f:
                f.write(file_bytes)
            
            # Display PDF using st.pdf with file path
            st.pdf(temp_pdf_path)
            
            # Also provide download option
            st.download_button(
                label="📥 Download PDF",
                data=file_bytes,
                file_name="invoice.pdf",
                mime="application/pdf"
            )
        else:
            st.image(file_bytes, use_column_width=True)
    except Exception as e:
        st.error(f"Error displaying file: {str(e)}")

def safe_decimal(value):
    """Safely convert value to decimal string, handling currency formatting"""
    try:
        if value is None:
            return "0"
        if isinstance(value, str) and value.strip().lower() == "none":
            return "0"
        # Remove currency symbols, commas, and whitespace
        cleaned = str(value).replace("$", "").replace(",", "").strip()
        return str(float(cleaned)) if cleaned else "0"
    except Exception:
        return "0"

def update_header_field(field_name, value):
    """Update a header field and track the change locally"""
    st.session_state.header_editable[field_name] = value
    st.session_state.header_changes[field_name] = value

def update_detail_field(detail_index, field_name, value):
    """Update a detail line item field and track the change locally"""
    if detail_index < len(st.session_state.detail_editable):
        st.session_state.detail_editable[detail_index][field_name] = value
        if detail_index not in st.session_state.detail_changes:
            st.session_state.detail_changes[detail_index] = {}
        st.session_state.detail_changes[detail_index][field_name] = value

def get_cached_validation():
    """Get validation result - recompute each time for accuracy"""
    # Always compute fresh to ensure comparison_field is correct
    validation_result = validate_invoice_totals(
        st.session_state.header_editable,
        st.session_state.detail_editable
    )
    return validation_result

def get_change_summary():
    """Get a summary of all local changes made by the user"""
    return {
        "header_changes": st.session_state.header_changes,
        "detail_changes": st.session_state.detail_changes,
        "total_header_changes": len(st.session_state.header_changes),
        "total_detail_changes": len(st.session_state.detail_changes)
    }

def save_to_tables(header_df, detail_df, file_path):
    """Save header and detail data to Snowflake Hybrid Tables in a single transaction."""
    try:
        invoice_id = str(uuid.uuid4())
        header_row = header_df.iloc[0]
        
        # Handle optional invoice date; allow NULL when blank
        invoice_date_val = (header_row.get("InvoiceDate") or "").strip()
        if invoice_date_val:
            invoice_date_expr = f"TO_DATE('{invoice_date_val}', 'YYYY-MM-DD')"
        else:
            invoice_date_expr = "NULL"
        
        header_insert = f"""
        INSERT INTO <DB_NAME>.<SCHEMA>.INVOICE_HEADER
        (INVOICE_ID, VENDOR_NAME, INVOICE_NO, PURCHASE_ORDER_NO, INVOICE_DATE,
         INVOICE_SUBTOTAL, INVOICE_TAX_AMOUNT, INVOICE_TOTAL, DEPOSIT_CREDIT_AMOUNT, UPLOADED_FILE_PATH)
        VALUES
        ('{invoice_id}',
         '{header_row["VendorName"].replace(chr(39), chr(39)+chr(39))}',
         '{header_row["InvoiceNo"].replace(chr(39), chr(39)+chr(39))}',
         '{header_row["PurchaseOrderNo"].replace(chr(39), chr(39)+chr(39))}',
         {invoice_date_expr},
         {safe_decimal(header_row.get("InvoiceSubtotal"))},
         {safe_decimal(header_row.get("InvoiceTaxAmount"))},
         {safe_decimal(header_row.get("InvoiceTotal"))},
         {safe_decimal(header_row.get("DepositCreditAmount"))},
         '{file_path.replace(chr(39), chr(39)+chr(39))}')
        """
        
        # Build a single multi-row VALUES insert for details
        detail_values_sql_parts = []
        for _, detail_row in detail_df.iterrows():
            description = (detail_row.get("Description") or "").strip()
            quantity = safe_decimal(detail_row.get("Quantity"))
            line_total = safe_decimal(detail_row.get("LineTotal"))
            
            # Skip completely empty lines
            if not description and float(quantity) == 0 and float(line_total) == 0:
                continue
            
            detail_id = str(uuid.uuid4())
            line_number = int(detail_row.get("LineNumber") or 0)
            detail_values_sql_parts.append(
                f"('{detail_id}', '{invoice_id}', {line_number}, "
                f"'{description.replace(chr(39), chr(39)+chr(39))}', {quantity}, {line_total})"
            )
        
        # Execute within a transaction for atomicity (important for hybrid tables)
        session.sql("BEGIN").collect()
        try:
            session.sql(header_insert).collect()
            
            if detail_values_sql_parts:
                detail_insert = (
                    "INSERT INTO <DB_NAME>.<SCHEMA>.INVOICE_DETAIL "
                    "(DETAIL_ID, INVOICE_ID, LINE_ITEM_NUMBER, ITEM_DESCRIPTION, QUANTITY, LINE_TOTAL) VALUES "
                    + ", ".join(detail_values_sql_parts)
                )
                session.sql(detail_insert).collect()
            
            session.sql("COMMIT").collect()
            
            # Log the file path that was saved
            st.info(f"📁 File path stored in database: {file_path}")
            
            return True, invoice_id
        except Exception:
            session.sql("ROLLBACK").collect()
            raise
    except Exception as e:
        st.error(f"Error saving to database: {str(e)}")
        return False, None

def validate_invoice_totals(header_editable, detail_editable):
    """Validate that sum of line items matches subtotal (if valid and > 0) or invoice total"""
    try:
        # Calculate sum of line totals
        line_total_sum = 0
        for detail in detail_editable:
            line_total = detail.get("LineTotal", "0")
            if line_total and str(line_total).strip():
                try:
                    # Use safe_decimal to handle currency formatting
                    parsed_value = float(safe_decimal(line_total))
                    line_total_sum += parsed_value
                except (ValueError, TypeError):
                    pass
        
        # Round to 2 decimal places to fix floating-point precision
        line_total_sum = round(line_total_sum, 2)
        
        # Get subtotal - check if it's valid and greater than 0
        subtotal_raw = header_editable.get("InvoiceSubtotal", "")
        subtotal = None
        
        if subtotal_raw and str(subtotal_raw).strip():
            try:
                subtotal_str = safe_decimal(subtotal_raw)
                subtotal_value = float(subtotal_str) if subtotal_str else 0
                # Only use subtotal if it's > 0
                if subtotal_value > 0:
                    subtotal = subtotal_value
            except (ValueError, TypeError):
                pass
        
        # Determine which total to compare against
        # DEBUG: Always use subtotal if it exists and is > 0
        if subtotal is not None and subtotal > 0:
            # Use subtotal if it's valid and greater than 0
            comparison_total = subtotal
            comparison_field = "InvoiceSubtotal"
        else:
            # Fall back to invoice total if subtotal is blank, 0, or invalid
            comparison_total = float(safe_decimal(header_editable.get("InvoiceTotal", "0")))
            comparison_field = "InvoiceTotal"
        
        # Compare with exact matching - line totals must match exactly (difference = 0)
        difference = abs(comparison_total - line_total_sum)
        tolerance = 0.0  # Require exact match
        
        return {
            "line_total_sum": line_total_sum,
            "comparison_total": comparison_total,
            "comparison_field": comparison_field,
            "subtotal": subtotal if subtotal is not None else 0,
            "invoice_total": float(safe_decimal(header_editable.get("InvoiceTotal", "0"))),
            "difference": difference,
            "matches": difference == 0,  # Exact match required - difference must be 0
            "tolerance": tolerance,
            "debug_subtotal_raw": str(subtotal_raw),
            "debug_subtotal_parsed": subtotal
        }
    except Exception as e:
        st.error(f"Error validating totals: {str(e)}")
        return None

# ====================== MAIN UI ======================

# Initialize variables (will be set from sidebar uploader)
uploaded_file = None
save_clicked = False

# ===== ALWAYS VISIBLE: UPLOAD SIDEBAR =====
st.sidebar.subheader("📤 Upload Invoice")
uploaded_file_sidebar = st.sidebar.file_uploader(
    "Choose Invoice",
    type=["pdf", "jpg", "jpeg", "png"],
    key="invoice_uploader_sidebar"
)

# Process sidebar uploaded file immediately
if uploaded_file_sidebar:
    file_id = uploaded_file_sidebar.name + str(uploaded_file_sidebar.size)
    if file_id != st.session_state.current_file_id or not st.session_state.ai_extract_completed:
        with st.spinner("⏳ Processing invoice..."):
            file_path = upload_file_to_stage(uploaded_file_sidebar)
            if file_path:
                st.session_state.file_path = file_path
                st.session_state.pdf_content = uploaded_file_sidebar.getvalue()
                extracted_data = extract_invoice_data(file_path.replace("@<DB_NAME>.<SCHEMA>.INVOICE_UPLOADS/", ""))
                if extracted_data:
                    st.session_state.extracted_data = extracted_data
                    header_df, detail_df = split_data_into_dataframes(extracted_data)
                    if header_df is not None and detail_df is not None:
                        st.session_state.header_df = header_df
                        st.session_state.detail_df = detail_df
                        st.session_state.header_editable = header_df.to_dict(orient="records")[0]
                        st.session_state.detail_editable = detail_df.to_dict(orient="records")
                        st.session_state.ai_extract_completed = True
                        st.session_state.current_file_id = file_id
                        st.rerun()

# Display layout if data exists
if st.session_state.header_df is not None and st.session_state.detail_df is not None:
    st.divider()
    
    # Main layout: 20% left (header) + 80% right (PDF + details)
    left_col, right_col = st.columns([0.2, 0.8], gap="medium")
    
    # ===== LEFT COLUMN: HEADER DATA =====
    with left_col:
        st.subheader("📋 Header Information")
        
        with st.container(border=True):
            # Vendor Name
            st.session_state.header_editable["VendorName"] = st.text_input(
                "Vendor Name",
                value=st.session_state.header_editable.get("VendorName", ""),
                key="vendor_name"
            )
            
            # Invoice No
            st.session_state.header_editable["InvoiceNo"] = st.text_input(
                "Invoice No",
                value=st.session_state.header_editable.get("InvoiceNo", ""),
                key="invoice_no"
            )
            
            # PO No
            st.session_state.header_editable["PurchaseOrderNo"] = st.text_input(
                "PO No",
                value=st.session_state.header_editable.get("PurchaseOrderNo", ""),
                key="po_no"
            )
            
            # Invoice Date
            st.session_state.header_editable["InvoiceDate"] = st.text_input(
                "Invoice Date (YYYY-MM-DD)",
                value=st.session_state.header_editable.get("InvoiceDate", ""),
                key="invoice_date"
            )
            
            st.divider()
            
            # Subtotal
            st.text_input(
                "Subtotal",
                value=str(st.session_state.header_editable.get("InvoiceSubtotal", "0")),
                key="subtotal",
                on_change=lambda: st.session_state.header_editable.update({"InvoiceSubtotal": st.session_state.subtotal})
            )
            
            # Tax Amount
            st.text_input(
                "Tax Amount",
                value=str(st.session_state.header_editable.get("InvoiceTaxAmount", "0")),
                key="tax_amount",
                on_change=lambda: st.session_state.header_editable.update({"InvoiceTaxAmount": st.session_state.tax_amount})
            )
            
            # Total
            st.text_input(
                "Total",
                value=str(st.session_state.header_editable.get("InvoiceTotal", "0")),
                key="total",
                on_change=lambda: st.session_state.header_editable.update({"InvoiceTotal": st.session_state.total})
            )
            
            # Deposit/Credit Amount
            st.text_input(
                "Deposit/Credit Amount",
                value=str(st.session_state.header_editable.get("DepositCreditAmount", "0")),
                key="deposit_credit",
                on_change=lambda: st.session_state.header_editable.update({"DepositCreditAmount": st.session_state.deposit_credit})
            )
        
        st.divider()
        
        # Save button
        if st.button("💾 Save to Database", key="save_button_sidebar", type="primary", use_container_width=True):
            save_clicked = True
        
        st.divider()
        
        # Green status messages at bottom
        if st.session_state.file_path:
            st.success(f"✅ File uploaded to stage:\n`{st.session_state.file_path.split('/')[-1]}`")
        
        st.success("✅ Invoice data extracted successfully!")
    
    # ===== RIGHT COLUMN: PDF + DETAILS =====
    with right_col:
        # Top section (70%): PDF/Image viewer
        st.subheader("📄 Invoice Document")
        
        with st.container(border=True, height=500):
            if st.session_state.file_path:
                file_ext = st.session_state.file_path.split('.')[-1].lower()
                if file_ext.lower() == "pdf":
                    # Display PDF from Snowflake stage
                    display_pdf_from_stage(st.session_state.file_path)
                else:
                    # Display image using in-memory bytes
                    display_pdf(st.session_state.pdf_content, file_ext)
        
        st.divider()
        
        # Validate totals (cached for quick refresh)
        validation_result = get_cached_validation()
        
        # DEBUG: Show what we're getting
        if validation_result:
            with st.expander("🔍 Debug Info"):
                st.write(f"**Raw Subtotal Input:** {validation_result.get('debug_subtotal_raw', 'N/A')}")
                st.write(f"**Parsed Subtotal:** {validation_result.get('debug_subtotal_parsed', 'N/A')}")
                st.write(f"**Subtotal (from result):** {validation_result.get('subtotal', 0)}")
                st.write(f"**Invoice Total:** {validation_result.get('invoice_total', 0)}")
                st.write(f"**Comparison Field:** {validation_result.get('comparison_field', 'N/A')}")
                st.write(f"**Comparison Total:** {validation_result.get('comparison_total', 0)}")
                st.write(f"**Line Total Sum:** {validation_result.get('line_total_sum', 0)}")
                st.write(f"**Difference:** {validation_result.get('difference', 0)}")
                st.write(f"**Matches:** {validation_result.get('matches', False)}")
        
        if validation_result and not validation_result.get("matches", False):
            try:
                comparison_field = validation_result.get('comparison_field', 'InvoiceTotal')
                subtotal_amt = validation_result.get('subtotal', 0)
                invoice_total_amt = validation_result.get('invoice_total', 0)
                
                # Format label based on which field is being used for comparison
                if subtotal_amt > 0 and comparison_field == "InvoiceSubtotal":
                    label_display = "Subtotal"
                    display_amount = subtotal_amt
                else:
                    label_display = "Invoice Total"
                    display_amount = invoice_total_amt
                
                st.warning(
                    f"⚠️ **Invoice Total Mismatch!**\n\n"
                    f"- Sum of Line Items: ${validation_result.get('line_total_sum', 0):.2f}\n"
                    f"- {label_display}: ${display_amount:.2f}\n"
                    f"- **Difference: ${validation_result.get('difference', 0):.2f}**",
                    icon="⚠️"
                )
            except Exception as e:
                st.error(f"Error displaying validation: {str(e)}")
        elif validation_result and validation_result.get("matches", False):
            try:
                comparison_field = validation_result.get('comparison_field', 'InvoiceTotal')
                subtotal_amt = validation_result.get('subtotal', 0)
                invoice_total_amt = validation_result.get('invoice_total', 0)
                
                # Format label based on which field is being used for comparison
                if subtotal_amt > 0 and comparison_field == "InvoiceSubtotal":
                    label_display = "Subtotal"
                    display_amount = subtotal_amt
                else:
                    label_display = "Invoice Total"
                    display_amount = invoice_total_amt
                
                st.success(
                    f"✅ Totals Match ({label_display}): ${display_amount:.2f}",
                    icon="✅"
                )
            except Exception as e:
                st.error(f"Error displaying validation: {str(e)}")
        
        # Bottom section (30%): Detail data
        st.subheader("📊 Line Items")
        
        # Add/Remove line items controls
        col_add, col_remove = st.columns([1, 1])
        
        with col_add:
            if st.button("➕ Add Line Item", key="add_line_item"):
                # Add a new blank line item
                new_line = {
                    "LineNumber": len(st.session_state.detail_editable) + 1,
                    "Description": "",
                    "Quantity": "0",
                    "LineTotal": ""
                }
                st.session_state.detail_editable.append(new_line)
                st.rerun()
        
        with col_remove:
            if len(st.session_state.detail_editable) > 1:
                if st.button("➖ Remove Last Item", key="remove_line_item"):
                    # Remove last line item
                    st.session_state.detail_editable.pop()
                    st.rerun()
        
        st.divider()
        
        # Editable detail table
        detail_data_list = []
        
        for idx in range(len(st.session_state.detail_editable)):
            col1, col2, col3, col4, col5 = st.columns([0.8, 2.5, 0.8, 0.8, 0.5])
            
            with col1:
                st.session_state.detail_editable[idx]["LineNumber"] = st.number_input(
                    "Line #",
                    value=st.session_state.detail_editable[idx].get("LineNumber", idx + 1),
                    key=f"line_num_{idx}",
                    disabled=True
                )
            
            with col2:
                st.session_state.detail_editable[idx]["Description"] = st.text_input(
                    "Description",
                    value=st.session_state.detail_editable[idx].get("Description", ""),
                    key=f"desc_{idx}"
                )
            
            with col3:
                st.session_state.detail_editable[idx]["Quantity"] = st.text_input(
                    "Qty",
                    value=str(st.session_state.detail_editable[idx].get("Quantity", "")),
                    key=f"qty_{idx}"
                )
            
            with col4:
                st.text_input(
                    "Total",
                    value=str(st.session_state.detail_editable[idx].get("LineTotal", "")),
                    key=f"linetotal_{idx}",
                    on_change=lambda idx=idx: st.session_state.detail_editable[idx].update({"LineTotal": st.session_state.get(f"linetotal_{idx}", "")}) if idx < len(st.session_state.detail_editable) else None
                )
            
            with col5:
                # Remove button for each line (only show if more than 1 line item)
                if len(st.session_state.detail_editable) > 1:
                    st.write("\u200b")  # Invisible Unicode space for alignment
                    if st.button("❌", key=f"remove_{idx}", help="Delete this line item"):
                        st.session_state.detail_editable.pop(idx)
                        st.rerun()
                else:
                    st.write("")

# ===== SAVE FUNCTIONALITY =====
if save_clicked and st.session_state.header_df is not None:
    with st.spinner("💾 Saving data to database..."):
        # Update DataFrames with edited values
        header_df = pd.DataFrame([st.session_state.header_editable])
        detail_df = pd.DataFrame(st.session_state.detail_editable)
        
        # Save to tables
        success, invoice_id = save_to_tables(header_df, detail_df, st.session_state.file_path)
        
        if success:
            st.success(f"✅ Invoice saved successfully! Invoice ID: {invoice_id}")
            st.balloons()
            # Clear change tracking after successful save
            st.session_state.header_changes = {}
            st.session_state.detail_changes = {}
        else:
            st.error("❌ Failed to save invoice data")
