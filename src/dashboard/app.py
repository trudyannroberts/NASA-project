import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from src.db.db_neo import fetch_neo  # your function to get NEO data


# -----------------------------
# Helper: Generate Earth sphere
# -----------------------------
def create_earth_sphere(resolution=50):
    theta = np.linspace(0, 2 * np.pi, resolution)
    phi = np.linspace(0, np.pi, resolution)
    theta, phi = np.meshgrid(theta, phi)

    R = 1
    x = R * np.sin(phi) * np.cos(theta)
    y = R * np.sin(phi) * np.sin(theta)
    z = R * np.cos(phi)

    return x, y, z


# -----------------------------
# Helper: Position NEOs around Earth
# -----------------------------

def position_neos_3d(df):
    r_min = 1.2
    r_max = 4

    # scale distance
    r_scaled = r_min + (df['miss_distance_km'] - df['miss_distance_km'].min()) / \
               (df['miss_distance_km'].max() - df['miss_distance_km'].min()) * (r_max - r_min)

    # random angles
    theta = np.random.uniform(0, 2 * np.pi, len(df))  # azimuthal angle
    phi = np.arccos(np.random.uniform(-1, 1, len(df)))  # polar angle (uniform on sphere)

    df['x'] = r_scaled * np.sin(phi) * np.cos(theta)
    df['y'] = r_scaled * np.sin(phi) * np.sin(theta)
    df['z'] = r_scaled * np.cos(phi)

    return df

# -----------------------------
# Main Streamlit function
# -----------------------------
def neo_dash():
    st.title("Near Earth Objects")

    neo_rows = fetch_neo()
    df = pd.DataFrame([row[1:7] for row in neo_rows],
                      columns=['name', 'min_diameter', 'max_diameter', 'is_potential_hazard',
                               'close_approach_date', 'miss_distance_km'])

    df['close_approach_date'] = pd.to_datetime(df['close_approach_date'])

    # -------------------------
    # Filter hazardous NEOs
    # -------------------------
    show_only_hazardous = st.checkbox("Show only potentially hazardous NEOs", value=False)
    if show_only_hazardous:
        df = df[df['is_potential_hazard']]

    # scale marker size
    min_size, max_size = 3, 18
    if df['max_diameter'].max() == df['max_diameter'].min():
        df['size_scaled'] = min_size
    else:
        df['size_scaled'] = min_size + \
                            (df['max_diameter'] - df['max_diameter'].min()) / \
                            (df['max_diameter'].max() - df['max_diameter'].min()) * (max_size - min_size)

    # color for hazard
    df['color'] = df.apply(lambda r: 'red' if r['is_potential_hazard'] else 'lightgrey', axis=1)

    # compute 3D positions
    df = position_neos_3d(df)

    # Earth sphere
    x_e, y_e, z_e = create_earth_sphere()

    fig = go.Figure()

    # Add Earth
    fig.add_trace(go.Surface(
        x=x_e, y=y_e, z=z_e,
        colorscale=[[0, 'blue'], [1, 'blue']],
        showscale=False,
        opacity=1
    ))

    # Add NEOs
    fig.add_trace(go.Scatter3d(
        x=df['x'], y=df['y'], z=df['z'],
        mode='markers',
        marker=dict(size=df['size_scaled'], color=df['color'], opacity=0.8),
        text=df['name'],
        hovertemplate="<b>%{text}</b><br>Distance: %{customdata[0]:,.0f} km<br>"
                      "Max diameter: %{customdata[1]:,.1f} m",
        customdata=np.stack([df['miss_distance_km'], df['max_diameter']], axis=1),
        name='NEOs'
    ))

    fig.update_layout(
        scene=dict(
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            zaxis=dict(visible=False),
        ),
        width=900,
        height=900,
        margin=dict(l=0, r=0, t=0, b=0)
    )

    st.plotly_chart(fig, use_container_width=True)



# Run app
neo_dash()
