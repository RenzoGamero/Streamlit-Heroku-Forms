# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash
from dash import html
from dash import dcc

import plotly.express as px
import pandas as pd

app = Dash(__name__)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
df = pd.DataFrame({
    "Fruit": ["Apples", "Oranges", "Bananas", "Apples", "Oranges", "Bananas"],
    "Amount": [4, 1, 2, 2, 4, 5],
    "City": ["SF", "SF", "SF", "Montreal", "Montreal", "Montreal"]
})

fig = px.bar(df, x="Fruit", y="Amount", color="City", barmode="group")

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for your data.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)





myKey = 'my_key'
        if myKey not in st.session_state:
            st.session_state[myKey] = False
        if st.session_state[myKey]:
            myBtn = st.button('Mostrar Opciones')
            st.session_state[myKey] = False
        else:
            myBtn = st.button('Ocultar Opciones')
            st.session_state[myKey] = True

        print('st.session_state[myKey]= ', st.session_state[myKey])
        if st.session_state[myKey] == True:
            T = st.multiselect('Columnas para Agregar t ', (ColAgregacion))




