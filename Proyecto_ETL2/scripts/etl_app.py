import streamlit as st
import pandas as pd
from etl_core import create_connection, extract_data, apply_transformations, load_data
from db_credentials import source_config, target_config

# Setup session state
if "df" not in st.session_state:
    st.session_state.df = None
if "query_run" not in st.session_state:
    st.session_state.query_run = False

st.set_page_config(page_title="ETL Chinook → DW_Chinook", layout="centered")
st.title("🔄 ETL App - Chinook → DW_Chinook")

# Step 1: Input the SQL query
st.header("1️⃣ Consulta SQL de origen")
user_query = st.text_area("Escribe la consulta SQL para obtener los datos del sistema OLTP (Chinook):")

if st.button("🔍 Ejecutar Consulta"):
    if not user_query.strip():
        st.error("❌ Por favor ingresa una consulta.")
    else:
        try:
            source_conn = create_connection(source_config)
            df = extract_data(source_conn, user_query)
            source_conn.close()

            st.session_state.df = df  # Save result
            st.session_state.query_run = True
            st.success("✅ Consulta ejecutada correctamente.")
        except Exception as e:
            st.error(f"❌ Error al ejecutar la consulta: {e}")
            st.session_state.df = None
            st.session_state.query_run = False

# Step 2+: Continue only if data was loaded
if st.session_state.df is not None and st.session_state.query_run:
    df = st.session_state.df

    st.write("Vista previa de los datos:")
    st.dataframe(df.head())

    st.header("🧪 Transformaciones por columna")

    selected_columns = st.multiselect("Selecciona las columnas a transformar:", df.columns.tolist())
    transformations = {}

    for col in selected_columns:
        with st.expander(f"⚙️ Transformaciones para '{col}'"):
            ops = st.multiselect(
                f"Operaciones para {col}",
                options=[
                    'lowercase', 'uppercase', 'extract_year',
                    'extract_month_name', 'extract_trimester', 'extract_day_name',
                    'concat...'
                ],
                key=f"ops_{col}"
            )
            processed_ops = []
            for op in ops:
                if op == 'concat...':
                    val = st.text_input(f"Texto a concatenar a '{col}':", key=f"concat_{col}")
                    processed_ops.append({'concat': val})
                else:
                    processed_ops.append(op)
            transformations[col] = processed_ops

    st.header("🛠️ Configurar y Ejecutar ETL")
    with st.form("etl_form"):
        dest_table = st.text_input("Nombre exacto de la tabla destino en DW_CHINOOK:")
        key_columns = st.multiselect("Selecciona las columnas clave para evitar duplicados:", df.columns.tolist())
        submitted = st.form_submit_button("🚀 Ejecutar ETL")

    if submitted:
        if not dest_table or not key_columns:
            st.error("❌ Debes ingresar el nombre de la tabla destino y al menos una columna clave.")
        else:
            try:
                df_transformed = apply_transformations(df.copy(), transformations)
                target_conn = create_connection(target_config)
                inserted = load_data(target_conn, dest_table, df_transformed, key_columns)
                target_conn.close()

                st.success(f"✅ ETL completado. {inserted} registros insertados.")
                if inserted == 0:
                    st.info("ℹ️ No se insertaron nuevos registros (duplicados existentes).")
            except Exception as e:
                st.error(f"❌ Error en el ETL: {e}")
