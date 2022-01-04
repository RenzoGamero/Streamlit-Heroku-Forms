import pandas as pd
import streamlit as st
import numpy as np


st.write('Test Form Ipress')

st.write('Pregunta 1')
st.write('Pregunta 2')
st.write('Pregunta 3')



chart_data = pd.DataFrame(
     np.random.randn(20, 3),
     columns=['a', 'b', 'c'])

st.line_chart(chart_data)